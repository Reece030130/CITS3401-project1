from ETL import file_loading, dim_table_creation, fact_table_creation

if __name__ == '__main__':
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    sheet_name2 = sheet_name2.join(sheet_name.select(["Crash ID", "Number Fatalities"]), on="Crash ID", how="left")
    dim_tables = []
    dim_tables_variables = [
        ['Day of week', 'Dayweek', 'Month', 'Year'], 
        ['State', 'SA4 Name 2021', 'National LGA Name 2021', 'National Remoteness Areas'],
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
        [sheet_name, dim_tables_variables[0], ["Year","Month"] , None, "DateID", "-9", "Date", "dim_date.csv"],  
        # dimension Geography
        [sheet_name, dim_tables_variables[1], None, None, "GeoID", "-9", "Geo", "dim_geography.csv"],
        # dimension fatalities
        [sheet_name2, dim_tables_variables[2], None, None, "FatalityId","-9", "Fatalities", "dim_fatalities.csv"],
        # dimension crash
        [sheet_name, dim_tables_variables[3],None, None, "CrashID", '-9',"Crash", "dim_crash.csv"],
        # dimension road
        [sheet_name2, dim_tables_variables[4], None, None, "RoadId", "-9", "Road", "dim_road_df.csv"],
        # dimension vehicle
        [sheet_name,dim_tables_variables[5], None, None, "VehicleId", "-9", "Vehicle", "dim_vehicle_df.csv"],
        # dimension Time
        [sheet_name2, dim_tables_variables[6], None, {"Time": r"(\d{2}:\d{2}:\d{2})"},"TimeID", "-9","Time", "dim_time.csv"],
        # dimension hoilday
        [sheet_name,dim_tables_variables[7], None, None, "HolidayID", "-9", "Holiday", "dim_holiday.csv"]
    ]
    for [sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, convert_name, csv_name] in dim_table_list:
        dim_tables.append(dim_table_creation(sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, convert_name, csv_name))
    fatal = dim_table_creation(sheet_name2, sheet_name2.columns ,None, {"Time": r"(\d{2}:\d{2}:\d{2})"}, "FatalityId","-9", "Fatality", "BITRE_Fatality.csv")
    for i in range(len(dim_tables_variables)):
        fatal = fact_table_creation(dim_tables[i], fatal, dim_tables_variables[i])
    fatal.write_csv("fact_table.csv")