// Command sonata is the CLI and daemon for local workflow orchestration.
package main

import (
	"os"

	"github.com/mcondie/sonata/internal/cli"
)

// Stamped at link time by the Makefile via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(cli.Execute(cli.BuildInfo{
		Version: version,
		Commit:  commit,
		Date:    date,
	}))
}
