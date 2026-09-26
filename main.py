import marimo

__generated_with = "0.23.6"
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

    return glob, pq


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
    return


if __name__ == "__main__":
    app.run()
