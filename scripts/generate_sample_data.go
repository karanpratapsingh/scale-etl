package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/go-faker/faker/v4"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

/**
*	Usage:
*		go run scripts/generate_sample_data.go <filename> <row_size>
*	Example:
*		go run scripts/generate_sample_data.go test.csv 10000
**/

type Product struct {
	ID      string `faker:"nanoid"`
	Name    string `faker:"word"`
	Price   int
	InStock bool
}

func main() {
	start := time.Now()

	faker.AddProvider("nanoid", func(v reflect.Value) (interface{}, error) {
		return gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 8)
	})

	filePath := os.Args[1]
	size, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	switch size {
	case 1_000_000_000: // 1 billion
		generateLargeSampleData(filePath, 100)
	case 100_000_000: // 100 million
		generateLargeSampleData(filePath, 10)
	default:
		generateSampleData(filePath, size, false)
	}

	duration := time.Since(start)
	fmt.Printf("Generated %d items in %s\n", size, duration)
}

// Generate large files with 10 million rows increment
func generateLargeSampleData(filePath string, count int) {
	os.RemoveAll(filePath)

	headerFilePath := fmt.Sprintf("%s_header_tmp", filePath)
	rowsFilePath := fmt.Sprintf("%s_rows_tmp", filePath)

	paths := []string{
		headerFilePath,
	}

	generateSampleData(headerFilePath, 0, false) // Header file
	generateSampleData(rowsFilePath, 10_000_000, true)

	for i := 0; i < count; i += 1 {
		paths = append(paths, rowsFilePath)
	}

	outputFile, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	// Combine all the files
	for _, path := range paths {
		inputFile, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer inputFile.Close()

		_, err = io.Copy(outputFile, inputFile)
		if err != nil {
			panic(err)
		}
	}

	os.RemoveAll(headerFilePath)
	os.RemoveAll(rowsFilePath)
}

func generateSampleData(path string, size int, skipHeader bool) {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var product Product

	if !skipHeader {
		header := []string{"id", "name", "price", "in_stock"}

		if err := writer.Write(header); err != nil {
			panic(err)
		}
	}

	for i := 0; i < size; i += 1 {
		if err := faker.FakeData(&product); err != nil {
			panic(err)
		}

		row := []string{
			product.ID,
			product.Name,
			fmt.Sprint(product.Price),
			fmt.Sprint(product.InStock),
		}

		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
}
