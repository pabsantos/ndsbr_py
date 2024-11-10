import pandas as pd

ndsbr_06 = pd.read_parquet("data/ndsbr.parquet")
ndsbr_sample = pd.read_parquet("data/ndsbr_sample.parquet")

ndsbr = pd.concat([ndsbr_sample, ndsbr_06])

ndsbr['trip'] = ndsbr['trip'].astype(str)
ndsbr['valid_time'] = ndsbr['valid_time'].astype(str)

ndsbr.to_parquet("data/ndsbr_full.parquet")
ndsbr.to_csv("data/ndsbr_full.csv")