// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/api"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func NewApiCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		brokers    []string
		configFile string
	)
	command := &cobra.Command{
		Use:   "api",
		Short: "Interact with the Redpanda API",
	}

	command.PersistentFlags().StringSliceVar(
		&brokers,
		"brokers",
		[]string{},
		"Comma-separated list of broker ip:port pairs",
	)
	command.PersistentFlags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	// The ideal way to pass common (global flags') values would be to
	// declare PersistentPreRun hooks on each command root (such as rpk
	// api), validating them there and them passing them down to its
	// subcommands. However, Cobra only executes the last hook defined in
	// the command chain. Since NewTopicCommand requires a PersistentPreRun
	// hook to initialize the sarama Client and Admin, it overrides whatever
	// PersistentPreRun hook was declared in a parent command.
	// An alternative would be to declare a global var to hold the global
	// flag's value, but this would require flattening out the package
	// hierarchy to avoid import cycles (parent command imports child
	// command's package, child cmd import parent cmd's package to access
	// the flag's value), but this leads to entangled code.
	// As a cleaner workaround, the brokers value has to be gotten through a
	// closure with references to the required values (the config file
	// path, the list of brokers passed through --brokers) to deduce the
	// actual brokers list to be used.
	configClosure := common.FindConfigFile(mgr, &configFile)
	brokersClosure := common.DeduceBrokers(
		fs,
		common.CreateDockerClient,
		configClosure,
		&brokers,
	)
	producerClosure := common.CreateProducer(brokersClosure, configClosure)
	clientClosure := common.CreateClient(fs, brokersClosure, configClosure)
	adminClosure := common.CreateAdmin(fs, brokersClosure, configClosure)

	command.AddCommand(api.NewStatusCommand(adminClosure))
	command.AddCommand(
		api.NewTopicCommand(fs, clientClosure, adminClosure),
	)
	command.AddCommand(
		api.NewProduceCommand(producerClosure),
	)
	command.AddCommand(
		api.NewConsumeCommand(clientClosure),
	)
	return command
}
