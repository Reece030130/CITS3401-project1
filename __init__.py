from ETL import file_loading, dim_table_creation, fact_table_creation,pl

if __name__ == '__main__':
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    sheet_name2 = sheet_name2.join(sheet_name.select(["Crash ID", "Number Fatalities"]), on="Crash ID", how="left")
    geo_name = {'SA4 Name 2021': 'Sa4 Name21', 'State':'Ste Name21', 'National LGA Name 2021': 'Lga Name21'}
    sheet_name = sheet_name.rename(geo_name)
    sheet_name2 = sheet_name2.rename(geo_name)
    sheet_name2 = sheet_name2.with_columns([
        pl.when(pl.col(col) == "-9")
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
        for col in sheet_name2.columns
    ])
    sheet_name2 = sheet_name2.fill_null("Unknown")
    sheet_name2.write_csv("cleaned_data.csv")

    replacement_dict = {
    'NSW': 'New South Wales',
    'NT': 'Northern Territory',
    'WA': 'Western Australia',
    'SA': 'South Australia',
    'ACT': 'Australian Capital Territory',
    'Tas': 'Tasmania',
    'Vic': 'Victoria',
    'Qld': 'Queensland',
    }
    sheet_name = sheet_name.with_columns(
        pl.col('Ste Name21').replace(replacement_dict).alias('Ste Name21')
        )
    sheet_name2 = sheet_name2.with_columns(
        pl.col('Ste Name21').replace(replacement_dict).alias('Ste Name21')
        )
    dim_tables = []
    dim_tables_variables = [
        ['Day of week', 'Dayweek', 'Month', 'Year'], 
        ['Ste Name21', 'Sa4 Name21', 'Lga Name21', 'National Remoteness Areas'],
        ["Age Group", "Age", "Gender"],
        ['Crash Type'],
        ["National Road Type", "Road User"],
        ["Bus Involvement", "Heavy Rigid Truck Involvement", "Articulated Truck Involvement"],
        ['Time of day','Time'],
        ["Christmas Period", "Easter Period"]
    ]
    
    # create fatalityID and perpare for the fact table.
    dim_table_list =[
        # dimension date
        [sheet_name, dim_tables_variables[0], ["Year","Month"] , None, "DateID", ["-9","Unknown"], ["Month","Year"], "Date", "dim_date.csv"],  
        # dimension Geography
        [sheet_name2, dim_tables_variables[1], None, None, "GeoID", [], [],"Geo", "dim_geography.csv"],
        # dimension fatalities
        [sheet_name2, dim_tables_variables[2], None, None, "FatalityPersonalInfoId",["-9","Unknown"],["Age"], "FatalitiesPI", "dim_fatalitiesPersonalInfo.csv"],
        # dimension crash
        [sheet_name, dim_tables_variables[3],None, None, "CrashID", [],[],"Crash", "dim_crash.csv"],
        # dimension road
        [sheet_name2, dim_tables_variables[4], None, None, "RoadId", ["-9","Unknown"],[], "Road", "dim_road_df.csv"],
        # dimension vehicle
        [sheet_name2,dim_tables_variables[5], None, None, "VehicleId", [],[], "Vehicle", "dim_vehicle_df.csv"],
        # dimension Time
        [sheet_name2, dim_tables_variables[6], None, {"Time": r"(\d{2}:\d{2}:\d{2})"},"TimeID",["-9","Unknown"],[],"Time", "dim_time.csv"],
        # dimension hoilday
        [sheet_name,dim_tables_variables[7], None, None, "HolidayID", ["-9","Unknown"],[], "Holiday", "dim_holiday.csv"]
    ]
    for [sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, castint_value ,convert_name, csv_name] in dim_table_list:
        dim_tables.append(dim_table_creation(sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, castint_value, convert_name, csv_name))
    fatal = dim_table_creation(sheet_name2, sheet_name2.columns ,None, {"Time": r"(\d{2}:\d{2}:\d{2})"}, "FatalityId",[],["Year","Month", "Age"],  "Fatality", "fact_table.csv")
    for i in range(len(dim_tables_variables)):
        fatal = fact_table_creation(dim_tables[i], fatal, dim_tables_variables[i])
    fatal.write_csv("fact_table.csv")