partition:
	go run main.go partition --file-path samples/sample_10m.csv --partition-size 100000

transform:
	go run main.go transform --file-path samples/sample_10m.csv --segment-size 10000

search:
	go run main.go search --file-path samples/sample_10m.csv --segment-size 10000 --pattern abc

load:
	go run main.go load --file-path samples/sample_10m.csv --pool-size 50 --script-path ./scripts/sample_load_script.sh

clean:
	go run main.go clean --file-path samples/sample_10m.csv

build:
	go build -o bin/csv-etl main.go

lint:
	go fmt ./...
	go vet ./...
	staticcheck ./...

generate_sample_data:
	go run scripts/generate_sample_data.go samples/sample_1k.csv 1000
	go run scripts/generate_sample_data.go samples/sample_10k.csv 10000
	go run scripts/generate_sample_data.go samples/sample_100k.csv 100000
	go run scripts/generate_sample_data.go samples/sample_1m.csv 1000000
	go run scripts/generate_sample_data.go samples/sample_10m.csv 10000000
	go run scripts/generate_sample_data.go samples/sample_100m.csv 100000000

	echo "Generated all sample data"
	du -sh samples/*

start_postgres:
	docker run -itd -e POSTGRES_USER=user -e POSTGRES_DB=db -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:16

benchmark_pandas:
	python scripts/pandas_benchmark.py