from ETL import file_loading, dim_table_creation

if __name__ == '__main__':
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    dim_table =[
        # dimension date
        [sheet_name, ['Day of week', 'Dayweek', 'Month', 'Year'], ["Year","Month"] , None, "DateID", "-9", "Date", "dim_date.csv"],  
        # dimension Geography
        [sheet_name, ['State', 'SA4 Name 2021', 'National LGA Name 2021', 'National Remoteness Areas'], None, None, "GeoID", "-9", "Geo", "dim_geography.csv"],
        # dimension fatalities
        [sheet_name2, ["Age Group", "Age", "Gender"], None, None, "FatalityId","-9", "Fatalities", "dim_fatalities.csv"],
        # dimension crash
        [sheet_name, ['Number Fatalities', 'Crash Type'],None, None, "CrashID", '-9',"Crash", "dim_crash.csv"],
        # dimension road
        [sheet_name2, ["Speed Limit", "National Road Type", "Road User"], None, None, "RoadId", "-9", "Road", "dim_road_df.csv"],
        # dimension vehicle
        [sheet_name2,["Bus Involvement", "Heavy Rigid Truck Involvement", "Articulated Truck Involvement"], None, None, "VehicleId", "-9", "Vehicle", "dim_vehicle_df.csv"],
        # dimension Time
        [sheet_name, ['Time of Day','Time'], None, {"Time": r"(\d{2}:\d{2}:\d{2})"},"TimeID", "-9","Time", "dim_time.csv"],
        # dimension hoilday
        [sheet_name,["Christmas Period", "Easter Period"], None, None, "HolidayID", "-9", "Holiday", "dim_holiday.csv"]
    ]
    for [sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, convert_name, csv_name] in dim_table:
        dim_table_creation(sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, convert_name, csv_name)

