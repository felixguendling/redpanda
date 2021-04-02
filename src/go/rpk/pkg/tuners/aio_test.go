// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners_test

import (
	"fmt"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

func TestMaxAIOEventsCheck(t *testing.T) {
	const maxAIOEventsFile = "/proc/sys/fs/aio-max-nr"
	const scriptPath = "/tune.sh"
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		expectChange   bool
		expected       int
		expectedErrMsg string
	}{
		{
			name: "it shouldn't do anything if current >= reference",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("20000000"),
					maxAIOEventsFile,
				)
				return err
			},
		},
		{
			name: "it shouldn't do anything if current == reference",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("1048576"),
					maxAIOEventsFile,
				)
				return err
			},
		},
		{
			name: "it should set the value if current < reference",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("1048575"),
					maxAIOEventsFile,
				)
				return err
			},
			expectChange: true,
			expected:     1048576,
		},
		{
			name:           "it should fail if the file is missing",
			expectedErrMsg: "/proc/sys/fs/aio-max-nr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			exec := executors.NewScriptRenderingExecutor(fs, scriptPath)
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(st, err)
			}
			tuner := tuners.NewMaxAIOEventsTuner(fs, exec)
			res := tuner.Tune()
			if tt.expectedErrMsg != "" {
				require.Contains(st, res.Error().Error(), tt.expectedErrMsg)
				return
			}
			require.NoError(st, res.Error())
			contents, err := afero.ReadFile(fs, scriptPath)
			require.NoError(st, err)
			expected := `#!/bin/bash

# Redpanda Tuning Script
# ----------------------------------
# This file was autogenerated by RPK

`
			if tt.expectChange {
				expected = expected + fmt.Sprintf(`echo '%d' > /proc/sys/fs/aio-max-nr
`,
					tt.expected,
				)
			}
			require.Exactly(st, expected, string(contents))
		})
	}
}
