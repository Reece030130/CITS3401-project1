import polars as pl # type: ignore


def file_loading(filename: str, sheet_name: str) -> pl.DataFrame:
    df = pl.read_excel(filename, sheet_id=0)[sheet_name]
    new_columns = df.row(1)
    sheet_name = df.rename({old: new for old, new in zip(df.columns, new_columns)})
    return sheet_name


def dim_date(sheet_name: pl.DataFrame):
    dim_date_df = sheet_name[['Day of week', 'Dayweek', 'Month', 'Year']].unique()
    dim_date_df = dim_date_df.filter(
        pl.fold(
            acc=True,  # Start with True (keep rows by default)
            function=lambda acc, col: acc & (col != -9),  # Keep rows where all columns are NOT -9
            exprs=[pl.col(col) for col in dim_date_df.columns]  # Apply to all columns
        )
    )
    dim_date_df = dim_date_df.sort(["Year", "Month"])
    dim_date_df = dim_date_df.with_columns(
        pl.format("Date{}", pl.arange(1, len(dim_date_df) + 1)).alias("DateId")
    )
    dim_date_df = dim_date_df.select(["DateId"] + [col for col in dim_date_df.columns if col != "DateId"])

    dim_date_df.write_csv("dim_dates.csv")
    pass


def dim_geography(sheet_name: pl.DataFrame):
    dim_geography_df = sheet_name[['State', 'SA4 Name 2021', 'National LGA Name 2021',
                                   'National Remoteness Areas']].unique()
    dim_geography_df = dim_geography_df.with_columns(
        pl.format("Geo{}", pl.arange(1, len(dim_geography_df) + 1)).alias("GeoId")
    )
    dim_geography_df = dim_geography_df.select(["GeoId"] + [col for col in dim_geography_df.columns if col != "GeoId"])
    dim_geography_df.rename({'SA4 Name 2021': 'SA4', 'National LGA Name 2021': 'National LGA'})
    dim_geography_df.write_csv("dim_geography.csv")
    pass


def dim_fatalities(sheet_name: pl.DataFrame):
    dim_fatalities_df = sheet_name[["Age Group", "Age", "Gender"]].unique()
    dim_fatalities_df = dim_fatalities_df.with_columns(
        pl.format("Fatalities{}", pl.arange(1, len(dim_fatalities_df) + 1)).alias("FatalityId")
    )
    dim_fatalities_df = dim_fatalities_df.select(["FatalityId"]+ [col for col in dim_fatalities_df.columns if col != "FatalityId"])
    dim_fatalities_df.write_csv("dim_fatalities.csv")

def dim_time(sheet_name: pl.DataFrame):
    dim_time_df = sheet_name[['Time of Day','Time']].unique()
    dim_time_df = dim_time_df.with_columns(
        pl.format("Time{}", pl.arange(1, len(dim_time_df)+1)).alias("TimeId"),
        pl.col("Time").str.extract(r"(\d{2}:\d{2}:\d{2})").str.strptime(pl.Time, "%H:%M:%S")
    )
    dim_time_df = dim_time_df.select(["TimeId"]+ [col for col in dim_time_df.columns if col != "TimeId"])
    dim_time_df.write_csv("dim_time_df.csv")





