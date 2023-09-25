package internal

import "flag"

type Args struct {
	ConfigPath string
	Debug      bool
}

func ParseArgs() Args {
	var args Args

	flag.StringVar(&args.ConfigPath, "config", "config.yaml", "Config path")
	flag.BoolVar(&args.Debug, "debug", false, "Debug mode")
	flag.Parse()

	return args
}
