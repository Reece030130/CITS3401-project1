import polars as pl # type: ignore


def file_loading(filename: str, sheet_name: str) -> pl.DataFrame:
    df = pl.read_excel(filename, sheet_id=0)[sheet_name]
    new_columns = df.row(1)
    df = df.rename({old: new for old, new in zip(df.columns, new_columns)}).slice(2)
    return df

def dim_table_creation(sheet_name: pl.DataFrame, dimension_variable: list, sort_list: list |None, extract : dict | None, dimension_primary_key: str, filter_value: list[str|int], castint_value: list[str] , convert_name: str, csv_name: str):
    dim_table_df = sheet_name.select(dimension_variable).unique()
    
    for i in filter_value:     
        dim_table_df = dim_table_df.filter(
            pl.fold(
                acc=True,
                function=lambda acc, col: acc & (col != i),
                exprs=[pl.col(col) for col in dim_table_df.columns]
            )
        )
    if sort_list:
        dim_table_df.sort(sort_list)
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
    if castint_value:
        for i in castint_value:
            dim_table_df = dim_table_df.with_columns(
                pl.when(pl.col(i).str.contains(r"^\d+$"))
                .then(pl.col(i).cast(pl.Int64, strict=False))
                .otherwise(-1)  # or .otherwise(pl.lit("Unknown")) if you want to keep the original text
                .alias(i)
            )
    dim_table_df.write_csv(csv_name)
    return dim_table_df

def fact_table_creation(dim_tables: pl.DataFrame, original_table: pl.DataFrame, dim_tables_variable: list[str]):
    fact_table = original_table.join(dim_tables, on = dim_tables_variable).drop(dim_tables_variable)
    return fact_table


