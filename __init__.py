from ETL import file_loading, dim_table_creation, fact_table_creation,pl,clean_items
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules

"""
This script implements an ETL (Extract, Transform, Load) process that:
  1. Loads fatal crash and fatality data from Excel files using pre-built ETL functions.
  2. Performs data cleaning, renaming, and transformation using Polars.
  3. Builds several dimension tables and a fact table.
  4. Uses Pandas and mlxtend to prepare transaction data for association rule mining.
  5. Mines frequent itemsets with the Apriori algorithm and generates association rules.
  6. Focuses on rules involving 'Road User' information and prints out the top rules.
"""
if __name__ == '__main__':
    # Load fatal crashes and fatalities data from Excel files
    sheet_name = file_loading("bitre_fatal_crashes_dec2024.xlsx","BITRE_Fatal_Crash")
    sheet_name2 =file_loading("bitre_fatalities_dec2024.xlsx", "BITRE_Fatality")
    
    # Join the fatalities data with the crash data to obtain "Number Fatalities"
    sheet_name2 = sheet_name2.join(sheet_name.select(["Crash ID", "Number Fatalities"]), on="Crash ID", how="left")
    
    # Standardize geographic column names by renaming them according to geo_name mapping
    geo_name = {'SA4 Name 2021': 'Sa4 Name21', 'State':'Ste Name21', 'National LGA Name 2021': 'Lga Name21'}
    sheet_name = sheet_name.rename(geo_name)
    sheet_name2 = sheet_name2.rename(geo_name)

    # For sheet_name2, replace any occurrence of the value "-9" with None across all columns
    sheet_name2 = sheet_name2.with_columns([
        pl.when(pl.col(col) == "-9")
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
        for col in sheet_name2.columns
    ])

    # Replace null values with the string "Unknown" in sheet_name2.
    sheet_name2 = sheet_name2.fill_null("Unknown")

    # Create a dictionary to convert state abbreviations into full names.
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

    # Apply the replacement on the 'Ste Name21' column in both datasets.
    sheet_name = sheet_name.with_columns(
        pl.col('Ste Name21').replace(replacement_dict).alias('Ste Name21')
        )
    sheet_name2 = sheet_name2.with_columns(
        pl.col('Ste Name21').replace(replacement_dict).alias('Ste Name21')
        )

    # Define lists of variables corresponding to each dimension in your data model.
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
    
    # Build a list of instructions to create each dimension table.
    # Each sub-list contains:
    # [source DataFrame, selected columns, sort columns (or None), extraction dict (or None),
    # primary key name, filter values to remove, list of columns to cast as int, prefix to add, and output CSV filename]
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

    # Loop through each dimension instruction and build the dimension table, storing them in the dim_tables list.
    for [sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, castint_value ,convert_name, csv_name] in dim_table_list:
        dim_tables.append(dim_table_creation(sheetname, dimension_variable, short_list, extract, dimension_primary_key, filter_value, castint_value, convert_name, csv_name))
    
    # Create a fact table by transforming the fatalities sheet.
    # This applies extraction on the "Time" column and generates a unique FatalityId.
    fatal = dim_table_creation(sheet_name2, sheet_name2.columns ,None, {"Time": r"(\d{2}:\d{2}:\d{2})"}, "FatalityId",[],["Year","Month", "Age", "Speed Limit"],  "Fatality", "fact_table.csv")
    fatal = fatal.with_columns(
    (pl.when((pl.col("Christmas Period") == "Yes") | (pl.col("Easter Period") == "Yes"))
     .then(1)
     .otherwise(0)).alias("is_holiday")
    )
    # Compute average age per crash (requires joining back with age per crash)
    # First, ensure Age is cast to float for averaging
    age_avg = (
        fatal
        .with_columns(pl.col("Age").cast(pl.Float64, strict=False))
        .group_by("Crash ID")
        .agg(pl.col("Age").mean().alias("avg_age"))
    )
    # Join avg_age into fatal table
    fatal = fatal.join(age_avg, on="Crash ID", how="left")
    df = fatal.to_pandas()
    for i in range(len(dim_tables_variables)):
        fatal = fact_table_creation(dim_tables[i], fatal, dim_tables_variables[i])
    # Save final fact table
    fatal.write_csv("fact_table.csv")
    # Select attributes + location info
    selected_columns = [
        "Crash Type", "Gender", "Age Group", "Time of day", "Speed Limit",
        "Road User", "Day of week", "avg_age","is_holiday", "Month"
    ]

    # Keep only available columns
    selected_columns = [col for col in selected_columns if col in df.columns]
    df_arm = df[selected_columns]
    df_arm = df_arm[~df_arm.isin(['Unknown']).any(axis=1)]
    # Create transactions (include "Unknown" values)
    transactions = df_arm.apply(lambda row: [f"{col}={row[col]}" for col in df_arm.columns], axis=1).tolist()

    # One-hot encode
    # Use TransactionEncoder from mlxtend to convert transaction lists into a one-hot encoded array.
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)

    # Optional: Mine frequent itemsets and association rules
    frequent_itemsets = apriori(df_encoded, min_support=0.02, use_colnames=True)
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.5)
    
    # Focus on rules where 'Road User=' is on RHS
    # Focus on rules where the consequents contain "Road User=" and both antecedents and consequents
    # have at most 2 items.
    road_user_rules = rules[
        (rules['consequents'].apply(lambda x: len(x) <= 2)) &
        (rules['antecedents'].apply(lambda x: len(x) <= 2)) &
        (rules['consequents'].apply(lambda x: any("Road User=" in item for item in x)))]
    top_rules = road_user_rules.sort_values(by=["confidence","lift"], ascending= False)
    top_rules_clean = top_rules.head(10).copy()
    top_rules_clean["antecedents"] = top_rules_clean["antecedents"].apply(clean_items)

# Show cleaned rules
    print(top_rules_clean[["antecedents", "consequents", "support", "confidence", "lift"]])
