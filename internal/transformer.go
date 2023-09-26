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
	case TransformTypeJSON:
		transformer = &JSONTransformer{fs, schema, counter}
	default:
		panic("invalid transform type")
	}

	printSchemaInfo(transformType, schema)
	return transformer
}

type DynamoDBTransformer struct {
	fs      FS
	schema  Schema
	counter Counter
}

func (dt *DynamoDBTransformer) Transform(batchNo int, records [][]string) {
	tableName := dt.schema.TableName

	requestItems := make(map[string]map[string][]map[string]map[string]map[string]map[string]any)
	requestItems["RequestItems"] = make(map[string][]map[string]map[string]map[string]map[string]any)
	requestItems["RequestItems"][tableName] = make([]map[string]map[string]map[string]map[string]any, 0)

	/**
	requestItems map represents the following dynamodb JSON structure
	{
	    "RequestItems": map
	        "TableName": array
	                "PutRequest": map
	                    "Item": map
	                        "Key": map
	                            "S": "columnValue"
	                        "columnName": map
	                            "S": "columnValue"
	}
	*/

	for _, record := range records {
		putRequest := make(map[string]map[string]map[string]map[string]any)
		attributes := make(map[string]map[string]any, len(record))
		item := make(map[string]map[string]map[string]any)

		for i, columnValue := range record {
			columnName := dt.schema.Columns[i]
			columnType := dt.schema.Types[columnName]

			if columnName == dt.schema.Key {
				attributes["Key"] = map[string]any{
					dynamodbTypes[columnType]: parseValue(columnValue, columnType),
				}
			}

			attributes[columnName] = map[string]any{
				dynamodbTypes[columnType]: parseValue(columnValue, columnType),
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

	dt.fs.writeFile(path, "json", jsonData)
}

type ParquetTransformer struct {
	fs FS
}

func (ParquetTransformer) Transform(batchNo int, records [][]string) {
	fmt.Println("parquet: process", len(records), "records for batch", batchNo)
}

type JSONTransformer struct {
	fs      FS
	schema  Schema
	counter Counter
}

func (jt JSONTransformer) Transform(batchNo int, records [][]string) {
	jsonRecords := make([]map[string]any, 0)

	for _, record := range records {
		jsonRecord := make(map[string]any)

		for i, columnValue := range record {
			columnName := jt.schema.Columns[i]
			columnType := jt.schema.Types[columnName]

			jsonRecord[columnName] = parseValue(columnValue, columnType)
		}
		jsonRecords = append(jsonRecords, jsonRecord)
	}

	jsonData, err := json.Marshal(jsonRecords)
	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("%d/%d", batchNo, jt.counter.get())

	jt.fs.writeFile(path, "json", jsonData)
}

// type CSVTransformer struct {
// 	fs FS
// }

// func (CSVTransformer) Transform(batchNo int, records [][]string) {

// }
