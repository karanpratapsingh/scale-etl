package internal

import (
	"encoding/json"
	"fmt"
	"os"
)

type Transformer interface {
	Transform(batchNo int, records [][]string)
}

func NewTransformer(fs FS, transformType TransformType, schema Schema, totalBatches int) Transformer {
	var transformer Transformer

	// Delete existing output directory
	if pathExists(fs.outputPath) {
		os.RemoveAll(fs.outputPath)
	}

	// Create the directories for batches
	for i := 1; i <= totalBatches; i += 1 {
		makeDirectory(fmt.Sprintf("%s/%d", fs.outputPath, i))
	}

	counter := NewCounter(0)

	switch transformType { // TODO: add json and csv (header in each file)
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

func (dt *DynamoDBTransformer) Transform(batchNo int, records [][]string) {
	tableName := dt.schema.TableName

	requestItems := make(map[string]map[string][]map[string]map[string]map[string]map[string]any, 1)
	requestItems["RequestItems"] = make(map[string][]map[string]map[string]map[string]map[string]any, 1)
	requestItems["RequestItems"][tableName] = make([]map[string]map[string]map[string]map[string]any, 0)

	/**
	requestItems map represents the following dynamodb JSON structure
	{
	    "RequestItems": map
	        "TableName": array
	                "PutRequest": map
	                    "Item": map
	                        "Key": map
	                            "S": "FieldName"
	                        "FieldName": map
	                            "S": "FieldName"
	}
	*/

	for _, record := range records {
		putRequest := make(map[string]map[string]map[string]map[string]any, 1)
		attributes := make(map[string]map[string]any, len(record))
		item := make(map[string]map[string]map[string]any, 1)

		for i, fieldValue := range record {
			fieldName := dt.schema.Fields[i]
			fieldType := dt.schema.Types[fieldName]

			if fieldName == dt.schema.Key {
				attributes["Key"] = map[string]any{
					dynamodbTypes[fieldType]: parseValue(fieldValue, fieldType),
				}
			}

			attributes[fieldName] = map[string]any{
				dynamodbTypes[fieldType]: parseValue(fieldValue, fieldType),
			}
		}

		item["Item"] = attributes
		putRequest["PutRequest"] = item
		requestItems["RequestItems"][tableName] = append(requestItems["RequestItems"][tableName], putRequest)
	}

	jsonData, err := json.Marshal(requestItems)
	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("%d/%d", batchNo, dt.counter.get())
	// path := fmt.Sprint(dt.counter.get())
	dt.fs.writeFile(path, "json", jsonData)
}

type ParquetTransformer struct {
	fs FS
}

func (ParquetTransformer) Transform(batchNo int, records [][]string) {
	fmt.Println("parquet: process", len(records), "records for batch", batchNo)
}
