import polars as pl # type: ignore


def file_loading(filename: str, sheet_name: str) -> pl.DataFrame:
    df = pl.read_excel(filename, sheet_id=0)[sheet_name]
    new_columns = df.row(1)
    df = df.rename({old: new for old, new in zip(df.columns, new_columns)}).slice(2)
    return df

def dim_table_creation(sheet_name: pl.DataFrame, dimension_variable: list, short_list: list |None, extract : dict | None, dimension_primary_key: str, filter_value: str|int, convert_name: str, csv_name: str):
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
    if extract:
        for  i in extract:      
            dim_table_df = dim_table_df.with_columns(
                pl.col(i).str.extract(extract[i]).str.strptime(pl.Time, "%H:%M:%S")
            )
    dim_table_df = dim_table_df.with_row_count(name=dimension_primary_key)
    dim_table_df = dim_table_df.with_columns(
        (pl.lit(convert_name) + (pl.col(dimension_primary_key) + 1).cast(pl.Utf8)).alias(dimension_primary_key)
    )
    dim_table_df = dim_table_df.select([dimension_primary_key] + [col for col in dim_table_df.columns if col != dimension_primary_key])
    dim_table_df.write_csv(csv_name)
    return dim_table_df

def fact_table_creation(dim_tables: pl.DataFrame, original_table: pl.DataFrame, dim_tables_variable: list[str]):
    fact_table = original_table.join(dim_tables, on = dim_tables_variable).drop(dim_tables_variable)
    return fact_table