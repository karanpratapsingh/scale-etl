package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/go-faker/faker/v4"
)

func main() {
	start := time.Now()

	file, err := os.Create("test.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	data := [][]string{
		{"name", "dob", "email", "phone_number", "address", "city", "state", "postal_code"},
	}

	for i := 0; i < 10_000_000; i += 1 {
		address := faker.GetRealAddress()

		row := []string{
			faker.Name(),
			faker.Date(),
			faker.Email(),
			faker.Phonenumber(),
			address.Address,
			address.City,
			address.State,
			address.PostalCode,
		}
		data = append(data, row)
	}

	for _, row := range data {
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Generation took %s\n", duration)
}
