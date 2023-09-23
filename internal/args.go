package internal

import "flag"

func Args(chunkSize int, batchSize int, file string) {
	// TODO: set default by file size
	flag.IntVar(&chunkSize, "chunk-size", 1_000_000, "Chunk size")
	flag.IntVar(&batchSize, "batch-size", 10_000, "Batch size")
	flag.StringVar(&file, "file", "", "File path")
	flag.Parse()

	if batchSize > chunkSize {
		panic("batch size cannot be bigger than chunk size")
	}

	if file == "" {
		panic("invalid file path")
	}
}
