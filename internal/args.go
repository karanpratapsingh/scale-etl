package internal

import "flag"

type Args struct {
	ConfigPath string
}

func ParseArgs() Args {
	var args Args

	flag.StringVar(&args.ConfigPath, "config", "config.yaml", "Config path")
	flag.Parse()

	return args
}
