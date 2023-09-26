generate:
	go run scripts/generate.go test.csv 10000000
	du -sh test.csv

run:
	go run main.go
