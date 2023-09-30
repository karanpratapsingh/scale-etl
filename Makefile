partitions:
	go run main.go partitions

transform:
	go run main.go transform

search:
	go run main.go search

clean:
	go run main.go clean

generate_sample_data:
	go run scripts/generate_sample_data.go samples/sample_1k.csv 1000
	du -sh samples/sample_1k.csv

	go run scripts/generate_sample_data.go samples/sample_10k.csv 10000
	du -sh samples/sample_10k.csv

	go run scripts/generate_sample_data.go samples/sample_100k.csv 100000
	du -sh samples/sample_100k.csv

	go run scripts/generate_sample_data.go samples/sample_1m.csv 1000000
	du -sh samples/sample_1m.csv

	go run scripts/generate_sample_data.go samples/sample_10m.csv 10000000
	du -sh samples/sample_10m.csv

	go run scripts/generate_sample_data.go samples/sample_100m.csv 100000000
	du -sh samples/sample_100m.csv

	echo "Generated all sample data"
	du -sh samples
