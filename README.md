```
                ->         ...
                            ->
        *       ->          ->
                            ->
                ->         ...
        .
        .
        L1      L2         L3
      file   Partitions   Segments
        N       M          K
```

**Features**

- Commands
  - Partition
  - Transform: dynamodb | parquet | json | csv
  - Load: call command from config for each batch (how many files concurrent?)
  - Search: regexp?

**Future scope**
 - Write logs to file
 - Remove a column
 - Add a derived column

**Challenges**
- In memory partitions?
- Line widths to seek in file for streaming!