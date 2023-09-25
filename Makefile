generate:
	go run scripts/generate.go
	du -sh test.csv

run:
	go run main.go
