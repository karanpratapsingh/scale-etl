package internal

import (
	"fmt"
	"strings"
	"time"
)

func MeasureExecTime(id string, function func()) {
	start := time.Now()
	function()
	duration := time.Since(start)
	fmt.Printf("%s [%s]\n", id, duration)
}

func printInputFileInfo(filePath string, delimiter rune) {
	inputFileSize := getFileSize(filePath)
	totalRows := countFileRows(filePath)

	fmt.Printf("File: %s, Size: %d MB, Rows: %d, Delimiter: %s\n", filePath, inputFileSize, totalRows, string(delimiter))
}

func printPartitionInfo(totalPartitions, partitionSize, totalBatches, batchSize int) {
	fmt.Printf("Partitions: %d, Size: %d\n", totalPartitions, partitionSize)
	fmt.Printf("Batches: %d, Size: %d\n", totalBatches, batchSize)
}

func printSchemaInfo(transformType TransformType, schema Schema) {
	columns := make([]string, len(schema.Fields))

	for i, fieldName := range schema.Fields {
		fieldType := schema.Types[fieldName]
		columns[i] = fmt.Sprintf("%s (%s)", fieldName, fieldType)
	}

	tableInfo := fmt.Sprintf("Table: %s", schema.TableName)
	keyInfo := fmt.Sprintf("Key: %s", schema.Key)
	transformInfo := fmt.Sprintf("Transform: %s", transformType)

	if transformType == TransformTypeDynamoDB {
		transformInfo = fmt.Sprintf("%s, %s, %s", transformInfo, tableInfo, keyInfo)
	}

	fmt.Println(transformInfo)
	fmt.Println("Columns:", strings.Join(columns, ", "))
}
