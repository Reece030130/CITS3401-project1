import polars as pl # type: ignore


def file_loading(filename: str, sheet_name: str) -> pl.DataFrame:
    df = pl.read_excel(filename, sheet_id=0)[sheet_name]
    new_columns = df.row(1)
    sheet_name = df.rename({old: new for old, new in zip(df.columns, new_columns)}).slice(2)
    return sheet_name

def dim_time(sheet_name: pl.DataFrame):
    dim_time_df = sheet_name[['Time of Day','Time']].unique()
    dim_time_df = dim_time_df.filter(
        pl.fold(
            acc=True,  # Start with True (keep rows by default)
            function=lambda acc, col: acc & (col != -9),  # Keep rows where all columns are NOT -9
            exprs=[pl.col(col) for col in dim_time_df.columns]  # Apply to all columns
        )
    )
    dim_time_df = dim_time_df.with_columns(
        pl.format("Time{}", pl.arange(1, len(dim_time_df)+1)).alias("TimeId"),
        pl.col("Time").str.extract(r"(\d{2}:\d{2}:\d{2})").str.strptime(pl.Time, "%H:%M:%S")
    )
    dim_time_df = dim_time_df.select(["TimeId"]+ [col for col in dim_time_df.columns if col != "TimeId"])
    dim_time_df.write_csv("dim_time_df.csv")

def dim_table_creation(sheet_name: pl.DataFrame, dimension_variable: list, short_list: list |None, dimension_primary_key: str, filter_value: str|int, convert_name: str, csv_name: str):
    dim_table_df = sheet_name.select(dimension_variable).unique()
    dim_table_df = dim_table_df.filter(
        pl.fold(
            acc=True,
            function=lambda acc, col: acc & (col != filter_value),
            exprs=[pl.col(col) for col in dim_table_df.columns]
        )
    )
    if short_list:
        dim_table_df.sort(short_list)
    dim_table_df = dim_table_df.with_row_count(name=dimension_primary_key)
    dim_table_df = dim_table_df.with_columns(
        (pl.lit(convert_name) + (pl.col(dimension_primary_key) + 1).cast(pl.Utf8)).alias(dimension_primary_key)
    )
    dim_table_df = dim_table_df.select([dimension_primary_key] + [col for col in dim_table_df.columns if col != dimension_primary_key])
    dim_table_df.write_csv(csv_name)
    return None