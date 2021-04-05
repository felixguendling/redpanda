v_cc_library(
  NAME serde
  SRCS
    logger.cc
    error.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::rphashing
    v::utils
    v::reflection
    absl::flat_hash_map
    v::compression
  )
add_subdirectory(test)
