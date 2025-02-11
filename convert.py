import polars as pl
import pandas as pd
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--input", type=str)
parser.add_argument("--output", type=str)

args = parser.parse_args()

# schema = [
#     pl.Utf8,
#     pl.Int64,
#     pl.Utf8,
#     pl.Utf8,
#     pl.Date,
#     pl.Int64,
#     pl.Utf8,
#     pl.Utf8,
#     pl.Utf8,
# ]

df = pl.read_csv(
    args.input,
    # schema_overrides=schema,
    ignore_errors=True,
).write_parquet(args.output)
