package internal

func CopySlice(input [][]string) [][]string {
	copiedSlice := make([][]string, len(input))
	copy(copiedSlice, input)

	return copiedSlice
}
