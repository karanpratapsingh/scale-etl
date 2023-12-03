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
| 100m | 3.839415084s  | 1000000        |
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

M3 Pro (32 GB):

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
| 100k | 34.7ms    |
| 1m   | 316.1ms   |
| 10m  | 3.020s    |
| 100m | 41.2236s  |
| 1b   | 462.6593s |

| Size | Speedup (Times) |
| ---- | --------------- |
| 100k | 6.94            |
| 1m   | 6.26            |
| 10m  | 7.56            |
| 100m | 10.72           |
| 1b   | 11.52           |

### Transform

| size | time     |
| ---- | -------- |
| 100k | 181.2ms  |
| 1m   | 789.4ms  |
| 10m  | 7.9503s  |
| 100m | 91.2204s |
| 1b   | 988.845s |

| Size | Improvement (Times) |
| ---- | ------------------- |
| 100k | 12.87               |
| 1m   | 9.09                |
| 10m  | 9.67                |
| 100m | 13.50               |
| 1b   | 14.06               |

### Search

| size | time       |
| ---- | ---------- |
| 100k | 212.7ms    |
| 1m   | 1.1510s    |
| 10m  | 11.5798s   |
| 100m | 146.9870s  |
| 1b   | 1492.5549s |

| Size | Improvement (Times) |
| ---- | ------------------- |
| 100k | 18.73               |
| 1m   | 11.98               |
| 10m  | 21.41               |
| 100m | 30.72               |
| 1b   | 30.68               |
