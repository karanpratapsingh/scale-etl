package internal

// TODO: make generic
func copySlice(input [][]string) [][]string {
	copiedSlice := make([][]string, len(input))
	copy(copiedSlice, input)

	return copiedSlice
}

func makeBatches[T any](items []T, batchSize int) [][]T {
	var batches [][]T
	N := len(items)

	for i := 0; i < N; i += batchSize {
		end := i + batchSize
		if end > N {
			end = N
		}
		batches = append(batches, items[i:end])
	}

	return batches
}
