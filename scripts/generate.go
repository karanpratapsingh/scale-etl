package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/go-faker/faker/v4"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

/**
*	Usage:
*		go run scripts/generate.go <filename> <size>
*	Example:
*		go run scripts/generate.go test.csv 10000
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

	size, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	file, err := os.Create(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var product Product

	data := [][]string{
		{"id", "name", "price", "in_stock"},
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
		data = append(data, row)
	}

	for _, row := range data {
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Generated %d items in %s\n", size, duration)
}
