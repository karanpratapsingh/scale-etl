package internal

import (
	"encoding/json"
	"fmt"
	"os"
)

type Transformer interface {
	Transform(records [][]string)
}

func NewTransformer(fs FS, transformType TransformType, schema Schema, tableName string) Transformer {
	var transformer Transformer

	if pathExists(fs.outputPath) { // Delete existing transform directory
		os.RemoveAll(fs.outputPath)
	}

	makeDirectory(fs.outputPath)

	counter := NewCounter(0)

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = &DynamoDBTransformer{fs, schema, counter, tableName}
	case TransformTypeParquet:
		transformer = &ParquetTransformer{fs}
	default:
		panic("invalid transform type")
	}

	fmt.Println("Transform type", transformType)
	return transformer
}

type DynamoDBTransformer struct {
	fs        FS
	schema    Schema
	counter   Counter
	tableName string
}

func (dt *DynamoDBTransformer) Transform(records [][]string) {
	requestItems := make(map[string]map[string][]map[string]map[string]map[string]map[string]any, 1)
	requestItems["RequestItems"] = make(map[string][]map[string]map[string]map[string]map[string]any, 1)
	requestItems["RequestItems"][dt.tableName] = make([]map[string]map[string]map[string]map[string]any, 0)

	for _, record := range records {
		putRequest := make(map[string]map[string]map[string]map[string]any, 1)
		attributes := make(map[string]map[string]any, len(record))
		item := make(map[string]map[string]map[string]any, 1)

		// TODO: define key in config
		// attributes["Key"] = map[string]string{
		// 	dynamodbTypes[fieldType]: value,
		// }

		for i, fieldValue := range record {
			fieldName := dt.schema.Fields[i]
			fieldType := dt.schema.Types[fieldName]

			attributes[fieldName] = map[string]any{
				dynamodbTypes[fieldType]: parseValue(fieldValue, fieldType),
			}
		}

		item["Item"] = attributes
		putRequest["PutRequest"] = item
		requestItems["RequestItems"][dt.tableName] = append(requestItems["RequestItems"][dt.tableName], putRequest)
	}

	jsonData, err := json.Marshal(requestItems)
	if err != nil {
		panic(err)
	}

	filename := dt.counter.get()

	// TODO: make it a function
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
