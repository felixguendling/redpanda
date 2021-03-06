// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/replicate_entries_stm.h"

#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "rpc/types.h"

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

ss::future<append_entries_request> replicate_entries_stm::share_request() {
    // one extra copy is needed for retries
    return with_semaphore(_share_sem, 1, [this] {
        return details::foreign_share_n(std::move(_req.batches), 2)
          .then([this](std::vector<model::record_batch_reader> readers) {
              // keep a copy around until the end
              _req.batches = std::move(readers.back());
              readers.pop_back();
              return append_entries_request{
                _req.node_id, _req.meta, std::move(readers.back())};
          });
    });
}
ss::future<result<append_entries_reply>> replicate_entries_stm::do_dispatch_one(
  vnode n,
  append_entries_request req,
  ss::lw_shared_ptr<std::vector<ss::semaphore_units<>>> units) {
    using ret_t = result<append_entries_reply>;

    if (n == _ptr->_self) {
        auto f = _ptr->flush_log()
                   .then([this, units]() {
                       auto lstats = _ptr->_log.offsets();
                       auto last_idx = lstats.committed_offset;
                       append_entries_reply reply;
                       reply.node_id = _ptr->_self;
                       reply.target_node_id = _ptr->_self;
                       reply.group = _ptr->group();
                       reply.term = _ptr->term();
                       // we just flushed offsets are the same
                       reply.last_dirty_log_index = last_idx;
                       reply.last_committed_log_index = last_idx;
                       reply.result = append_entries_reply::status::success;
                       return ret_t(std::move(reply));
                   })
                   .handle_exception(
                     []([[maybe_unused]] const std::exception_ptr& ex) {
                         return ret_t(errc::leader_flush_failed);
                     });

        _dispatch_sem.signal();
        return f;
    }
    return send_append_entries_request(n, std::move(req));
}

clock_type::time_point replicate_entries_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_replicate_append_timeout;
}

ss::future<result<append_entries_reply>>
replicate_entries_stm::send_append_entries_request(
  vnode n, append_entries_request req) {
    _ptr->update_node_append_timestamp(n);
    vlog(_ctxlog.trace, "Sending append entries request {} to {}", req.meta, n);

    req.target_node_id = n;
    auto f = _ptr->_client_protocol
               .append_entries(
                 n.id(),
                 std::move(req),
                 rpc::client_opts(append_entries_timeout()))
               .then([this](result<append_entries_reply> reply) {
                   return _ptr->validate_reply_target_node(
                     "append_entries_replicate", std::move(reply));
               });
    _dispatch_sem.signal();
    return f.finally(
      [this, n] { _ptr->suppress_heartbeats(n, _followers_seq[n], false); });
}

ss::future<> replicate_entries_stm::dispatch_one(
  vnode id, ss::lw_shared_ptr<std::vector<ss::semaphore_units<>>> units) {
    return ss::with_gate(
             _req_bg,
             [this, id, units]() mutable {
                 return dispatch_single_retry(id, std::move(units))
                   .then([this, id](result<append_entries_reply> reply) {
                       auto it = _followers_seq.find(id);
                       auto seq = it == _followers_seq.end()
                                    ? follower_req_seq(0)
                                    : it->second;
                       if (!reply) {
                           _ptr->get_probe().replicate_request_error();
                       }
                       _ptr->process_append_entries_reply(
                         id.id(), reply, seq, _dirty_offset);
                   });
             })
      .handle_exception_type([](const ss::gate_closed_exception&) {});
}

ss::future<result<append_entries_reply>>
replicate_entries_stm::dispatch_single_retry(
  vnode id, ss::lw_shared_ptr<std::vector<ss::semaphore_units<>>> units) {
    return share_request()
      .then([this, id, units](append_entries_request r) mutable {
          return do_dispatch_one(id, std::move(r), units);
      })
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(_ctxlog.warn, "Error while replicating entries {}", e);
          return result<append_entries_reply>(
            errc::append_entries_dispatch_error);
      });
}

ss::future<result<storage::append_result>>
replicate_entries_stm::append_to_self() {
    return share_request()
      .then([this](append_entries_request req) mutable {
          vlog(_ctxlog.trace, "Self append entries - {}", req.meta);
          _ptr->_last_write_consistency_level = consistency_level::quorum_ack;
          return _ptr->disk_append(
            std::move(req.batches), consensus::update_last_quorum_index::yes);
      })
      .then([](storage::append_result res) {
          return result<storage::append_result>(std::move(res));
      })
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(
            _ctxlog.warn,
            "Error replicating entries, leader append failed - {}",
            e);
          return result<storage::append_result>(errc::leader_append_failed);
      });
}
/**
 *  We skip sending follower requests it those two cases:
 *   - follower is recovering - when follower is not fully caught up it will not
 *     accept append entries request, missing data will be replicated to
 *     follower during recovery process
 *   - we haven't received any response from the follower for replicate append
 *     timeout duration - follower is probably down, we will not be able to
 *     send the request to the follower and it will require recovery. This
 *     prevents pending follower request queue build up and relieve memory
 *     pressure. Follower will still receive heartbeats, as we skip sending
 *     append entries request, after recovery follower will start receiving
 *     requests.
 */
inline bool replicate_entries_stm::should_skip_follower_request(vnode id) {
    if (auto it = _ptr->_fstats.find(id); it != _ptr->_fstats.end()) {
        const auto timeout = clock_type::now()
                             - _ptr->_replicate_append_timeout;

        return it->second.last_hbeat_timestamp < timeout
               || it->second.is_recovering;
    }

    return false;
}

ss::future<result<replicate_result>>
replicate_entries_stm::apply(ss::semaphore_units<> u) {
    // first append lo leader log, no flushing
    auto cfg = _ptr->config();
    cfg.for_each_broker_id([this](const vnode& rni) {
        // suppress follower heartbeat, before appending to self log
        if (rni != _ptr->_self) {
            _ptr->suppress_heartbeats(rni, _followers_seq[rni], true);
        }
    });
    return append_to_self()
      .then([this, u = std::move(u), cfg = std::move(cfg)](
              result<storage::append_result> append_result) mutable {
          if (!append_result) {
              return ss::make_ready_future<result<storage::append_result>>(
                append_result);
          }
          _dirty_offset = append_result.value().last_offset;
          // dispatch requests to followers & leader flush
          std::vector<ss::semaphore_units<>> vec;
          vec.push_back(std::move(u));
          auto units = ss::make_lw_shared<std::vector<ss::semaphore_units<>>>(
            std::move(vec));
          uint16_t requests_count = 0;
          cfg.for_each_broker_id(
            [this, &requests_count, units](const vnode& rni) {
                // We are not dispatching request to followers that are
                // recovering
                if (should_skip_follower_request(rni)) {
                    vlog(
                      _ctxlog.trace,
                      "Skipping sending append request to {}",
                      rni);
                    _ptr->suppress_heartbeats(rni, _followers_seq[rni], false);
                    return;
                }
                ++requests_count;
                (void)dispatch_one(rni, units); // background
            });
          // Wait until all RPCs will be dispatched
          return _dispatch_sem.wait(requests_count)
            .then([append_result, units]() mutable { return append_result; });
      })
      .then([this](result<storage::append_result> append_result) {
          if (!append_result) {
              return ss::make_ready_future<result<replicate_result>>(
                append_result.error());
          }
          // this is happening outside of _opsem
          // store offset and term of an appended entry
          auto appended_offset = append_result.value().last_offset;
          auto appended_term = append_result.value().last_term;
          /**
           * we have to finish replication when committed offset is greater or
           * equal to the appended offset or when term have changed after
           * commit_index update, if that happend it means that entry might
           * have been either commited or truncated
           */
          auto stop_cond = [this, appended_offset, appended_term] {
              return _ptr->committed_offset() >= appended_offset
                     || _ptr->term() > appended_term;
          };
          return _ptr->_commit_index_updated.wait(stop_cond).then(
            [this, appended_offset, appended_term] {
                return process_result(appended_offset, appended_term);
            });
      });
}

result<replicate_result> replicate_entries_stm::process_result(
  model::offset appended_offset, model::term_id appended_term) {
    using ret_t = result<replicate_result>;
    vlog(
      _ctxlog.trace,
      "Replication result [offset: {}, term: {}, commit_idx: "
      "{}, "
      "current_term: {}]",
      appended_offset,
      appended_term,
      _ptr->committed_offset(),
      _ptr->term());

    // if term has changed we have to check if entry was
    // replicated
    if (unlikely(appended_term != _ptr->term())) {
        if (_ptr->_log.get_term(appended_offset) != appended_term) {
            return ret_t(errc::replicated_entry_truncated);
        }
    }
    vlog(
      _ctxlog.trace,
      "Replication success, last offset: {}, term: {}",
      appended_offset,
      appended_term);
    return ret_t(
      replicate_result{.last_offset = model::offset(appended_offset)});
}

ss::future<> replicate_entries_stm::wait() { return _req_bg.close(); }

replicate_entries_stm::replicate_entries_stm(
  consensus* p,
  append_entries_request r,
  absl::flat_hash_map<vnode, follower_req_seq> seqs)
  : _ptr(p)
  , _req(std::move(r))
  , _followers_seq(std::move(seqs))
  , _share_sem(1)
  , _ctxlog(_ptr->_ctxlog) {}

replicate_entries_stm::~replicate_entries_stm() {
    vassert(
      _req_bg.get_count() <= 0 || _req_bg.is_closed(),
      "Must call replicate_entries_stm::wait(). is_gate_closed:{}",
      _req_bg.is_closed());
}

} // namespace raft
