partition:
	go run main.go partition --file-path samples/sample_10m.csv --partition-size 100000

transform:
	go run main.go transform --file-path samples/sample_10m.csv --segment-size 10000

search:
	go run main.go search --file-path samples/sample_10m.csv --segment-size 10000 --pattern abc

clean:
	go run main.go clean --file-path samples/sample_10m.csv

build:
	go build -o bin/csv-etl main.go

generate_sample_data:
	go run scripts/generate_sample_data.go samples/sample_1k.csv 1000
	go run scripts/generate_sample_data.go samples/sample_10k.csv 10000
	go run scripts/generate_sample_data.go samples/sample_100k.csv 100000
	go run scripts/generate_sample_data.go samples/sample_1m.csv 1000000
	go run scripts/generate_sample_data.go samples/sample_10m.csv 10000000
	go run scripts/generate_sample_data.go samples/sample_100m.csv 100000000

	echo "Generated all sample data"
	du -sh samples/*
