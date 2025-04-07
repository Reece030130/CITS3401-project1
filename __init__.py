from ETL import file_loading, dim_table_creation, fact_table_creation

if __name__ == '__main__':
    # Load datasets
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx", "BITRE_Fatal_Crash")
    sheet_name2 = file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")

    # Join crash data into the fatality sheet for enrichment
    sheet_name2 = sheet_name2.join(
        sheet_name.select(["Crash ID", "Number Fatalities"]),
        on="Crash ID",
        how="left"
    )

    # Clean column names in crash data
    sheet_name = sheet_name.rename({col: col.strip() for col in sheet_name.columns})

    # Dimension variable definitions
    dim_tables_variables = [
        ['Day of week', 'Dayweek', 'Month', 'Year'],  # Date
        ['State', 'SA4 Name 2021', 'National LGA Name 2021', 'National Remoteness Areas'],  # Geography
        ["Age Group", "Age", "Gender"],  # Fatalities
        ['Crash Type'],  # Crash
        ["National Road Type", "Road User"],  # Road
        ["Bus Involvement", "Heavy Rigid Truck Involvement", "Articulated Truck Involvement"],  # Vehicle
        ['Time of day', 'Time'],  # Time
        ["Christmas Period", "Easter Period"]  # Holiday
    ]

    # Define how each dimension is created
    dim_table_list = [
        [sheet_name, dim_tables_variables[0], ["Year", "Month"], None, "DateID", "-9", "Date", "dim_date.csv"],
        [sheet_name, dim_tables_variables[1], None, None, "GeoID", "-9", "Geo", "dim_geography.csv"],
        [sheet_name2, dim_tables_variables[2], None, None, "FatalityId", "-9", "Fatalities", "dim_fatalities.csv"],
        [sheet_name, dim_tables_variables[3], None, None, "CrashID", "-9", "Crash", "dim_crash.csv"],
        [sheet_name2, dim_tables_variables[4], None, None, "RoadId", "-9", "Road", "dim_road_df.csv"],
        [sheet_name2, dim_tables_variables[5], None, None, "VehicleId", "-9", "Vehicle", "dim_vehicle_df.csv"],  # FIXED: source now sheet_name2
        [sheet_name2, dim_tables_variables[6], None, {"Time": r"(\d{2}:\d{2}:\d{2})"}, "TimeID", "-9", "Time", "dim_time.csv"],
        [sheet_name, dim_tables_variables[7], None, None, "HolidayID", "-9", "Holiday", "dim_holiday.csv"]
    ]

    # Create dimension tables
    dim_tables = []
    for [sheet, variables, short_list, extract, pk, filter_val, name, filename] in dim_table_list:
        dim_tables.append(
            dim_table_creation(sheet, variables, short_list, extract, pk, filter_val, name, filename)
        )

    # Create enriched fatality dataset
    fatal = dim_table_creation(
        sheet_name2,
        sheet_name2.columns,
        None,
        {"Time": r"(\d{2}:\d{2}:\d{2})"},
        "FatalityId",
        "-9",
        "Fatality",
        "BITRE_Fatality.csv"
    )

    # Join dimension tables into fact table
    for i in range(len(dim_tables_variables)):
        fatal = fact_table_creation(dim_tables[i], fatal, dim_tables_variables[i])

    # Save final fact table
    fatal.write_csv("fact_table.csv")
