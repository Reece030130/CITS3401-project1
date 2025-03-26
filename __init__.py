from ETL import file_loading, dim_geography, dim_date, dim_fatalities, dim_time

if __name__ == '__main__':
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    # dim_geography(sheet_name)
    # dim_date(sheet_name)
    dim_fatalities(sheet_name2)
    dim_time(sheet_name)

