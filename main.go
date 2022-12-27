//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// spanner-truncate is a tool to delete all rows from the tables in a Cloud Spanner database without deleting tables themselves.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/cloudspannerecosystem/spanner-truncate/truncate"
	"github.com/jessevdk/go-flags"
)

type options struct {
	ProjectID     string `short:"p" long:"project" env:"SPANNER_PROJECT_ID" description:"(required) GCP Project ID."`
	InstanceID    string `short:"i" long:"instance" env:"SPANNER_INSTANCE_ID" description:"(required) Cloud Spanner Instance ID."`
	DatabaseID    string `short:"d" long:"database" env:"SPANNER_DATABASE_ID" description:"(required) Cloud Spanner Database ID."`
	Quiet         bool   `short:"q" long:"quiet" description:"Disable all interactive prompts."`
	Tables        string `short:"t" long:"tables" description:"Comma separated table names to be truncated. Default to truncate all tables if not specified."`
	ExcludeTables string `short:"e" long:"exclude-tables" description:"Comma separated table names to be exempted from truncating. 'tables' and 'exclude-tables' cannot co-exist"`
}

const maxTimeout = time.Hour * 24

func main() {
	var opts options
	if _, err := flags.Parse(&opts); err != nil {
		exitf("Invalid options\n")
	}

	if opts.ProjectID == "" || opts.InstanceID == "" || opts.DatabaseID == "" {
		exitf("Missing options: -p, -i, -d are required.\n")
	}

	var targetTables []string
	var excludeTables []string
	if opts.Tables != "" {
		targetTables = strings.Split(opts.Tables, ",")
	}

	if opts.ExcludeTables != "" {
		if opts.Tables != "" {
			exitf("Conflict: --tables and --exclude-tables cannot be both set.\n")
		}
		excludeTables = strings.Split(opts.ExcludeTables, ",")
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()
	go handleInterrupt(cancel)

	if err := truncate.Run(ctx, opts.ProjectID, opts.InstanceID, opts.DatabaseID, opts.Quiet, os.Stdout, targetTables, excludeTables); err != nil {
		exitf("ERROR: %s", err.Error())
	}
}

func exitf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

func handleInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	cancel()
}
