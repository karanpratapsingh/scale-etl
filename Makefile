run:
	go run main.go

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

	cat samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv samples/sample_10m.csv > sample_100m.csv
	du -sh samples/sample_100m.csv

	echo "Generated all sample data"
	du -sh samples
