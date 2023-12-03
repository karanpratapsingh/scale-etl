import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

num_threads = os.cpu_count()


def partition_benchmark(input_file: str, partition_size: int):
    start_time = time.time()
    df = pd.read_csv(input_file)
    total_rows = len(df)
    num_partitions = (total_rows + partition_size - 1) // partition_size

    os.makedirs("output", exist_ok=True)
    partition_manifest = {"total_rows": total_rows, "partitions": []}

    def get_partition_points(start_row, end_row):
        return {"start": start_row, "end": end_row}

    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(num_partitions):
            start_row = i * partition_size
            end_row = min((i + 1) * partition_size - 1, total_rows - 1)

            futures.append(executor.submit(get_partition_points, start_row, end_row))

        for future in as_completed(futures):
            partition_manifest["partitions"].append(future.result())

    json_result = json.dumps(partition_manifest, indent=4)
    with open("output/partition_manifest.json", "w") as json_file:
        json_file.write(json_result)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\tPartition: {elapsed_time:.4f}s")


def transform_benchmark(input_file: str, output_folder: str, segment_size: int):
    start_time = time.time()
    df = pd.read_csv(input_file)
    os.makedirs(output_folder, exist_ok=True)

    num_files = len(df) // segment_size + 1

    def split_and_save(df: pd.DataFrame, start_idx: int, end_idx: int, file_path: str):
        df.iloc[start_idx:end_idx].to_csv(file_path, index=False)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        for i in range(num_files):
            start_idx = i * segment_size
            end_idx = (i + 1) * segment_size
            file_path = os.path.join(output_folder, f"output_{i + 1}.csv")

            futures.append(
                executor.submit(split_and_save, df, start_idx, end_idx, file_path)
            )

        for future in futures:
            future.result()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\tTransform: {elapsed_time:.4f}s")


def search_benchmark(input_file: str, output_file: str, search_string: str):
    start_time = time.time()
    df = pd.read_csv(input_file)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    def search_column(column_name):
        filtered_df = df[
            df[column_name]
            .astype(str)
            .str.contains(search_string, case=False, na=False)
        ]
        return filtered_df

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {
            executor.submit(search_column, column): column for column in df.columns
        }

        results = pd.concat([future.result() for future in as_completed(futures)])

    results.to_csv(output_file, index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\tSearch: {elapsed_time:.4f}s")


if __name__ == "__main__":
    benchmarks = [  # [file_path, segment size, partition_size]
        ["samples/sample_100k.csv", 10_000, 10_000],
        ["samples/sample_1m.csv", 10_000, 100_000],
        ["samples/sample_10m.csv", 10_000, 100_000],
        ["samples/sample_100m.csv", 100_000, 1_000_000],
        ["samples/sample_1b.csv", 100_000, 1_000_000],
    ]

    for i, (input_file, segment_size, partition_size) in enumerate(benchmarks):
        output_folder = f"output/{i}"

        print(input_file)
        partition_benchmark(input_file, partition_size)
        transform_benchmark(input_file, output_folder, segment_size)
        search_benchmark(input_file, "output/matches.csv", "abc")
