import polars as pl # type: ignore

"""Overall Purpose
Code uses the Polars library to perform an ETL (Extract, Transform, Load) process where:
Load an Excel file and prepare the raw DataFrame
Create dimension tables (with filtering, sorting, transformation, primary key creation, and type casting)
Join to build a fact table from the original dataset and the dimension table
Clean up items in a list based on a splitting operation
Each function is designed to handle one part of the data pipeline.
"""
"""
Loads an Excel file, selects a specific sheet, renames columns based on values from a particular row, and slices off header rows that are not needed.
"""

"""
Load and preprocess an Excel sheet into a Polars DataFrame.

This function reads an Excel file, selects a specified sheet, and renames
the columns based on the values from the second row. It then slices off
the first two rows, which are assumed to contain original headers or
metadata, to prepare the DataFrame for further processing.

Parameters:
    filename (str): The path to the Excel file.
    sheet_name (str): The name of the sheet to load.

Returns:
    pl.DataFrame: A Polars DataFrame with renamed columns and sliced rows.
"""
def file_loading(filename: str, sheet_name: str) -> pl.DataFrame:
    df = pl.read_excel(filename, sheet_id=0)[sheet_name]
    new_columns = df.row(1)
    df = df.rename({old: new for old, new in zip(df.columns, new_columns)}).slice(2)
    return df

"""
Create a dimension table from a Polars DataFrame.

This function processes a given DataFrame to create a dimension table by
selecting specified columns, filtering out unwanted values, sorting, and
performing transformations such as extracting and parsing time data. It
also generates a primary key for the dimension table, casts specified
columns to integers, and writes the resulting table to a CSV file.

Parameters:
    sheet_name (pl.DataFrame): The input DataFrame to process.
    dimension_variable (list): Columns to include in the dimension table.
    sort_list (list | None): Columns to sort the dimension table by.
    extract (dict | None): Mapping of columns to regex patterns for extraction.
    dimension_primary_key (str): Name of the primary key column.
    filter_value (list[str | int]): Values to filter out from the table.
    castint_value (list[str]): Columns to cast to integer type.
    convert_name (str): Prefix for the primary key values.
    csv_name (str): Filename for the output CSV.

Returns:
    pl.DataFrame: The processed dimension table as a Polars DataFrame.
"""
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
    dim_table_df = dim_table_df.with_row_index(name=dimension_primary_key)
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

"""
Create a fact table by joining dimension tables with the original table.

This function performs a join operation between the original table and
dimension tables based on specified columns. It then removes the columns
used for joining from the resulting fact table.

Parameters:
    dim_tables (pl.DataFrame): The dimension tables to join with.
    original_table (pl.DataFrame): The original table to be joined.
    dim_tables_variable (list[str]): The columns to join on.

Returns:
    pl.DataFrame: The resulting fact table after the join operation.
"""
def fact_table_creation(dim_tables: pl.DataFrame, original_table: pl.DataFrame, dim_tables_variable: list[str]):
    fact_table = original_table.join(dim_tables, on = dim_tables_variable).drop(dim_tables_variable)
    return fact_table

"""
Clean items in a list by splitting each item and extracting the last segment.

This function processes a list of strings, where each string is expected
to contain an '=' character. It splits each string at the '=' character
and returns a list containing only the segments after the '='.

Parameters:
    itemset (list[str]): A list of strings to be processed.

Returns:
    list[str]: A list of strings containing the segments after the '='.
"""
def clean_items(itemset):
    return [item.split("=")[-1] for item in itemset]

