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

### Transform

**Benchmark** (with non-linear trend graph)

Batch size 10, Segment size (10k for 100k), (100k for 1m-100m), (1m for 1b)

| size | time          | batch size | segment size |
| ---- | ------------- | ---------- | ------------ |
| 100k | 14.083667ms   | 20         | 10000        |
| 1m   | 86.953584ms   | 20         | 10000        |
| 10m  | 821.929875ms  | 20         | 10000        |
| 100m | 6.751803375s  | 20         | 100000       |
| 1b   | 70.230209375s | 20         | 100000       |

### Search

**Benchmark** (with non-linear trend graph)

TODO: after regexp
| size | time |
| ---- | ------------- |
| 100k | |
| 1m | |
| 10m | |
| 100m | |
| 1b | |

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
