# TODO: title

## Abstract

## Introduction

### Background

### Research Gap

## Technology Used

Why Go?

- compiles to any arch
- small binaries
- good concurrency primitives
  <ref: csp paper>

## Test data

## Implementation

The proposed system is implemented as a Command Line Interface (CLI)...

### Partition

**Benchmark** (with non-linear trend graph)

Partition size (10k for 100k), (100k for 1m-10m), (1m for 100m/1b)

| size | time          | partition size |
| ---- | ------------- | -------------- |
| 100k | 5.049708ms    | 10000          |
| 1m   | 50.612875ms   | 100000         |
| 10m  | 398.853792ms  | 100000         |
| 100m | 3.844415084s  | 1000000        |
| 1b   | 40.162212292s | 1000000        |

| size | time     | partition size |
| ---- | -------- | -------------- |
| 100k | 5.2ms    | 10000          |
| 1m   | 39.4ms   | 100000         |
| 10m  | 362.36ms | 100000         |
| 100m | 3.36s    | 1000000        |
| 1b   | 35.08s   | 1000000        |

### Transform

**Benchmark** (with non-linear trend graph)

| size | batch size | segment size | time       |
| ---- | ---------- | ------------ | ---------- |
| 100k | 10         | 10000        | 14.0836ms  |
| 1m   | 10         | 10000        | 86.9535ms  |
| 10m  | 20         | 10000        | 821.9298ms |
| 100m | 20         | 100000       | 6.7518s    |
| 1b   | 20         | 100000       | 70.2302s   |

| size | time     | batch size | segment size |
| ---- | -------- | ---------- | ------------ |
| 100k | 12.83ms  | 10         | 10000        |
| 1m   | 53.73ms  | 10         | 10000        |
| 10m  | 408.61ms | 20         | 10000        |
| 100m | 7.8s     | 20         | 100000       |
| 100m | 4.59s    | 100        | 100000       |
| 1b   | 79s      | 20         | 100000       |
| 1b   | 48.3s    | 100        | 100000       |

### Search

**Benchmark** (with non-linear trend graph)

| size | segment size | time       |
| ---- | ------------ | ---------- |
| 100k | 10000        | 12.1287ms  |
| 1m   | 10000        | 96.2360ms  |
| 10m  | 10000        | 541.4265ms |
| 100m | 100000       | 4.7879s    |
| 1b   | 100000       | 48.6179s   |

### Load

**Benchmark** (with non-linear trend graph)

## Testing and Benchmarks

Below results are average of 5 runs for sample csv files of total rows ranging from 100 thousand to 1 Billion.

The CLI tool was tested on <machine> with <memory> Memory and <cpu> CPU.

https://www.reddit.com/r/cpp/comments/g7aflx/update_towards_a_fast_singlethreaded_csv_parser/

## Challenges

- In memory partitions without reading the entire file buffer
- Line widths to seek in file for streaming!
- Line by line read and load becomes a bottleneck as each data store has it's limits (eg. dynamo db)

## Future scope

- Remove a column.
- Add/Derive a column from existing column.
- DLQ with backoff on load failed.

## Conclusion

## Single threaded (Go)

### Transform

| size | time       |
| ---- | ---------- |
| 100k | 37.5163ms  |
| 1m   | 266.6583ms |
| 10m  | 2.8439s    |
| 100m | 14.1503s   |
| 1b   | 108.2863s  |

### Search

| size | time       |
| ---- | ---------- |
| 100k | 28.5316ms  |
| 1m   | 190.7396ms |
| 10m  | 1.9537s    |
| 100m | 12.3020s   |
| 1b   | 78.9591s   |

## Single threaded (Pandas)

```python
old_values = []
new_values = []

results = [
    (
        round(((old - new) / old) * 100, 2),  # Percentage Improvement
        round(old / new, 2)  # Speedup (Times)
    )
    for old, new in zip(old_values, new_values)
]

for i, (percentage_improvement, speedup) in enumerate(results, 1):
    print(f"Value {i}:")
    print(f"  Percentage Improvement: {percentage_improvement}%")
    print(f"  Speedup (Times): {speedup}\n")

```

### Partition

| size | time      |
| ---- | --------- |
| 100k | 0.0347s   |
| 1m   | 0.3161s   |
| 10m  | 3.0200s   |
| 100m | 41.2236s  |
| 1b   | 462.6593s |

| Size | Percentage Improvement | Speedup (Times) |
| ---- | ---------------------- | --------------- |
| 100k | 85.61%                 | 6.94            |
| 1m   | 83.99%                 | 6.26            |
| 10m  | 86.78%                 | 7.56            |
| 100m | 90.69%                 | 10.72           |
| 1b   | 91.28%                 | 11.52           |

### Transform

samples/sample_1b.csv:

| size | time     |
| ---- | -------- |
| 100k | 0.1048s  |
| 1m   | 0.7894s  |
| 10m  | 7.9503s  |
| 100m | 91.2204s |
| 1b   | 988.845s |

| Size | Percentage Improvement | Speedup (Times) |
| ---- | ---------------------- | --------------- |
| 100k | 98.57%                 | 69.87           |
| 1m   | 89.01%                 | 9.09            |
| 10m  | 89.68%                 | 9.67            |
| 100m | 92.59%                 | 13.50           |
| 1b   | 92.92%                 | 14.06           |

### Search

| size | time       |
| ---- | ---------- |
| 100k | 0.1212s    |
| 1m   | 1.1510s    |
| 10m  | 11.5798s   |
| 100m | 146.9870s  |
| 1b   | 1492.5549s |

| Size | Percentage Improvement | Speedup (Times) |
| ---- | ---------------------- | --------------- |
| 100k | 99.01%                 | 100.00          |
| 1m   | 91.63%                 | 11.98           |
| 10m  | 95.33%                 | 21.41           |
| 100m | 96.73%                 | 30.72           |
| 1b   | 96.75%                 | 30.68           |
