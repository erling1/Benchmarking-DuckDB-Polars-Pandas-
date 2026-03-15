import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyarrow as pa 
    import pyarrow.parquet as pq
    import duckdb
    import glob 
    import pyarrow.dataset as ds
    import os
    import time
    import polars as pl
    from datetime import datetime, timezone

    return ds, duckdb, glob, os, pl, pq, time


@app.cell
def _(glob, pq):
    ban_events_path = "archive/ban_events.parquet"
    chats_path = "archive/chats_2021-01.parquet"
    super_chats_path = "archive/superchats_2021-03.parquet"

    ban_events = pq.read_table(ban_events_path)
    chats = pq.read_table(chats_path)
    super_chats = pq.read_table(super_chats_path)

    all_superchats = glob.glob("**/*superchats*", recursive=True) 
    all_chats = [p for p in glob.glob("**/*chats*", recursive=True) if "superchats" not in p]
    return all_superchats, chats


@app.cell
def _(chats):
    chats
    return


@app.cell
def _(chats):
    chats.nbytes / 1024**3
    return


@app.cell
def _():
    string_timestamp_files = [
        "archive/chats_2022-02.parquet",
        "archive/chats_2021-07.parquet",
        "archive/chats_2022-03.parquet",
        "archive/chats_2021-04.parquet",
        "archive/chats_2022-01.parquet",
        "archive/chats_2021-05.parquet",
        "archive/chats_2021-10.parquet",
        "archive/chats_2022-05.parquet",
        "archive/chats_2021-09.parquet",
        "archive/chats_2022-04.parquet",
        "archive/chats_2021-08.parquet",
        "archive/chats_2021-11.parquet",
        "archive/chats_2021-03.parquet",
        "archive/chats_2021-12.parquet",
    ]
    return (string_timestamp_files,)


@app.cell
def _(all_superchats, ds, string_timestamp_files):
    chats_dataset = ds.dataset(string_timestamp_files, format="parquet")
    superchats_dataset = ds.dataset(all_superchats, format="parquet")
    return (chats_dataset,)


@app.cell
def _(chats_dataset, os):
    sum([os.path.getsize(f) for f in chats_dataset.files]) / 1024**3
    #sum([os.path.getsize(f) for f in superchats_dataset.files]) / 1024**3
    return


@app.cell
def _(duckdb, time):
    #COUNT_SQL_DUCKDB = """select count(*) from chats_dataset where timestamp='2022-02-01T00:00:00.094000+00:00' """
    COUNT_SQL_DUCKDB = """SELECT SUM(YEAR(timestamp::TIMESTAMP)) FROM chats_dataset"""


    with duckdb.connect() as con:
        start_duck = time.time()
        count_duck = con.execute(COUNT_SQL_DUCKDB).to_arrow_table()
        duckdb_elapsed = time.time() - start_duck

    #total rows: 1 556 187 472
    return (count_duck,)


@app.cell
def _(count_duck):
    count_duck
    return


@app.cell
def _(pl, string_timestamp_files):
    pl.scan_parquet(string_timestamp_files).select(pl.col("timestamp").str.slice(0, 4).cast(pl.Int32).sum()).explain()
    return


@app.cell
def _(pl, string_timestamp_files, time):
    start_polar = time.time()
    sum_polar = (
        pl.scan_parquet(string_timestamp_files)
        .select(pl.col("timestamp").str.slice(0, 4).cast(pl.Int32).sum())
        .collect(streaming=True)
        .item()
    )
    polars_elapsed = time.time() - start_polar
    return (polars_elapsed,)


@app.cell
def _(polars_elapsed):
    polars_elapsed
    return


@app.cell
def _(pl, string_timestamp_files):
    print(pl.scan_parquet(string_timestamp_files).explain())
    return


@app.cell
def _(file_list, pd, start, time):
    start_pandas = time.time()
    total = 0
    for chunk in pd.read_parquet(file_list, chunksize=100_000):
        total += len(chunk[chunk["timestamp"] == ""])
    elapsed_pandas = time.time() - start
    return


if __name__ == "__main__":
    app.run()
