from ETL import file_loading, dim_time, dim_table_creation

if __name__ == '__main__':
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    dim_time(sheet_name)
    dim_table =[
        [sheet_name, ['Day of week', 'Dayweek', 'Month', 'Year'], ["Year","Month"] ,"DateID", "-9", "Date", "dim_date.csv"],  
        [sheet_name, ['State', 'SA4 Name 2021', 'National LGA Name 2021', 'National Remoteness Areas'], None, "GeoID", "-9", "Geo", "dim_geography.csv"],
        [sheet_name2, ["Age Group", "Age", "Gender"], None, "FatalityId","-9", "Fatalities", "dim_fatalities.csv"],
        [sheet_name, ['Number Fatalities', 'Crash Type'],None, "CrashID", '-9',"Crash", "dim_crash.csv"],
        [sheet_name2, ["Speed Limit", "National Road Type", "Road User"], None, "RoadId", "-9", "Road", "dim_road_df.csv"],
        [sheet_name2,["Bus Involvement", "Heavy Rigid Truck Involvement", "Articulated Truck Involvement"], None, "VehicleId", "-9", "Vehicle", "dim_vehicle_df.csv"]
    ]
    for [sheetname, dimension_variable, short_list, dimension_primary_key, filter_value, convert_name, csv_name] in dim_table:
        dim_table_creation(sheetname, dimension_variable, short_list, dimension_primary_key, filter_value, convert_name, csv_name)

