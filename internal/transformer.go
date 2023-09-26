package internal

import (
	"encoding/json"
	"fmt"
	"os"
)

type Transformer interface {
	Transform(records [][]string)
}

func NewTransformer(fs FS, transformType TransformType, schema Schema) Transformer {
	var transformer Transformer

	if pathExists(fs.outputPath) { // Delete existing transform directory
		os.RemoveAll(fs.outputPath)
	}

	makeDirectory(fs.outputPath)

	counter := NewCounter(0)

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = &DynamoDBTransformer{fs, schema, counter}
	case TransformTypeParquet:
		transformer = &ParquetTransformer{fs}
	default:
		panic("invalid transform type")
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct {
	fs      FS
	schema  Schema
	counter Counter
}

func (dt *DynamoDBTransformer) Transform(records [][]string) {
	// TODO: config: table name is required for dynamodb
	// TODO: write parser with schema

	jsonData, err := json.Marshal(records)
	if err != nil {
		panic(err)
	}

	filename := dt.counter.get()

	path := fmt.Sprintf("%s/%d.json", dt.fs.outputPath, filename)

	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if _, err = file.Write(jsonData); err != nil {
		panic(err)
	}
}

type ParquetTransformer struct {
	fs FS
}

func (ParquetTransformer) Transform(records [][]string) {
	fmt.Println("parquet: process", len(records))
}
