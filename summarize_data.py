import polars as pl

path = "data/raw/proofread_connections_783.feather"

# Streaming scan without full data load
df_lazy = pl.scan_ipc(path)
print("Schema:", df_lazy.schema)

# Count rows without loading all
row_count = df_lazy.select(pl.len()).collect().item()
print("Total rows:", row_count)
