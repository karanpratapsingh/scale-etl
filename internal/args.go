package internal

import "flag"

type Args struct {
	ChunkSize int
	BatchSize int
	FilePath  string
}

func ParseArgs() Args {
	var args Args

	flag.IntVar(&args.ChunkSize, "chunk", 10_000, "Chunk size")
	flag.IntVar(&args.BatchSize, "batch", 1_000, "Batch size")
	flag.StringVar(&args.FilePath, "file", "", "File path")
	flag.Parse()

	if args.BatchSize > args.ChunkSize {
		panic("batch size cannot be bigger than chunk size")
	}

	if args.FilePath == "" {
		panic("invalid file path")
	}

	return args
}
