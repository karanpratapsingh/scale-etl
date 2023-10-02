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

func PrintInputFileInfo(filePath string, totalRows int) {
	inputFileSize := getFileSize(filePath)

	fmt.Printf("File: %s, Size: %f MB, Rows: %d\n", filePath, inputFileSize, totalRows)
}

func PrintSchemaInfo(schema Schema) {
	columns := make([]string, len(schema.Columns))

	for i, columnName := range schema.Columns {
		columnType := schema.Types[columnName]
		columns[i] = fmt.Sprintf("%s (%s)", columnName, columnType)
	}

	fmt.Println("Columns:", strings.Join(columns, ", "))
}

func PrintTransformInfo(schema Schema, transformType TransformType, delimiter rune) {
	tableInfo := fmt.Sprintf("Table: %s", schema.TableName)
	keyInfo := fmt.Sprintf("Key: %s", schema.Key)
	transformInfo := fmt.Sprintf("Transform: %s", transformType)

	if transformType == TransformTypeDynamoDB {
		transformInfo = fmt.Sprintf("%s, %s, %s", transformInfo, tableInfo, keyInfo)
	}

	fmt.Println(transformInfo)
}

func PrintBatchInfo(totalBatches, batchSize int) {
	fmt.Printf("Batches: %d, Size: %d\n", totalBatches, batchSize)
}

func PrintSegmentInfo(segmentSize int) {
	fmt.Printf("Segment size: %d\n", segmentSize)
}

func printPartitionInfo(totalPartitions, partitionSize int) {
	fmt.Printf("Partitions: %d, Size: %d\n", totalPartitions, partitionSize)
}

func printSearchInfo(pattern, outputPath string) {
	fmt.Printf("Pattern: %s, Output: %s\n", pattern, outputPath)
}
