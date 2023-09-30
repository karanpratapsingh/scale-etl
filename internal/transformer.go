package internal

import (
	"encoding/json"
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Transformer interface {
	BatchProcessor
}

func NewTransformer(output Output, transformType TransformType, schema Schema) Transformer {
	var transformer Transformer

	switch transformType {
	case TransformTypeDynamoDB:
		transformer = DynamoDBTransformer{output, schema}
	case TransformTypeParquet:
		metadata := buildParquetMetadata(schema)
		transformer = ParquetTransformer{output, schema, metadata}
	case TransformTypeJSON:
		transformer = JSONTransformer{output, schema}
	case TransformTypeCSV:
		transformer = CSVTransformer{output, schema}
	default:
		panic(ErrTransformTypeNotSupported(transformType))
	}

	PrintSchemaInfo(schema)
	return transformer
}

type DynamoDBTransformer struct {
	output Output
	schema Schema
}

func (cs DynamoDBTransformer) BatchComplete(int) {}

func (dt DynamoDBTransformer) ProcessSegment(batchNo int, rows []Row) {
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

	for _, row := range rows {
		putRequest := make(map[string]map[string]map[string]map[string]any)
		attributes := make(map[string]map[string]any)
		item := make(map[string]map[string]map[string]any)

		for i, columnValue := range row {
			columnName := dt.schema.Columns[i]
			columnType := dt.schema.Types[columnName]

			if columnName == dt.schema.Key {
				attributes["Key"] = map[string]any{
					DynamodbTypes[columnType]: parseValue(columnValue, columnType),
				}
			}

			attributes[columnName] = map[string]any{
				DynamodbTypes[columnType]: parseValue(columnValue, columnType),
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

	dt.output.writeSegmentFile(batchNo, jsonData, ExtensionTypeJSON)
}

type ParquetTransformer struct {
	output   Output
	schema   Schema
	metadata []string
}

func buildParquetMetadata(schema Schema) []string {
	typeMetadata := make([]string, 0)

	for _, columnName := range schema.Columns { // Order matters
		columnType := schema.Types[columnName]

		meta := fmt.Sprintf("name=%s, %s", columnName, ParquetTypes[columnType])
		typeMetadata = append(typeMetadata, meta)
	}

	return typeMetadata
}

func (cs ParquetTransformer) BatchComplete(int) {}

func (pt ParquetTransformer) ProcessSegment(batchNo int, rows []Row) {
	filePath := pt.output.getSegmentFilePath(batchNo, ExtensionTypeParquet)

	lfw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		panic(err)
	}

	writer, err := writer.NewCSVWriter(pt.metadata, lfw, 1) // use only 1 goroutine
	if err != nil {
		panic(err)
	}

	for _, row := range rows {
		record := make([]*string, len(row))

		for i := 0; i < len(row); i++ {
			record[i] = &row[i]
		}

		if err := writer.WriteString(record); err != nil {
			panic(err)
		}
	}

	if err := writer.WriteStop(); err != nil {
		panic(err)
	}

	if err := lfw.Close(); err != nil {
		panic(err)
	}
}

type JSONTransformer struct {
	output Output
	schema Schema
}

func (cs JSONTransformer) BatchComplete(int) {}

func (jt JSONTransformer) ProcessSegment(batchNo int, row []Row) {
	jsonRows := make([]map[string]any, 0)

	for _, row := range row {
		jsonRow := make(map[string]any)

		for i, columnValue := range row {
			columnName := jt.schema.Columns[i]
			columnType := jt.schema.Types[columnName]

			jsonRow[columnName] = parseValue(columnValue, columnType)
		}
		jsonRows = append(jsonRows, jsonRow)
	}

	jsonData, err := json.Marshal(jsonRows)
	if err != nil {
		panic(err)
	}

	jt.output.writeSegmentFile(batchNo, jsonData, ExtensionTypeJSON)
}

type CSVTransformer struct {
	output Output
	schema Schema
}

func (cs CSVTransformer) BatchComplete(int) {}

func (ct CSVTransformer) ProcessSegment(batchNo int, rows []Row) {
	rows = append([]Row{ct.schema.Columns}, rows...) // Prepend header
	ct.output.writeSegmentFile(batchNo, rows, ExtensionTypeCSV)
}
