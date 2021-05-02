import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime

#%%

# This is code for processing the rolling sales data file from nyc.gov (the file with all the boroughs)
nyc_gov_df = pd.read_csv("data/real_estate/nyc_gov_data/nyc_gov_real_estate.csv")
nyc_gov_df['SALE DATE'] = pd.to_datetime(nyc_gov_df['SALE DATE'])
nyc_gov_df['BOROUGH'] = nyc_gov_df['BOROUGH'].astype(np.int64)
nyc_gov_df['ZIP CODE'] = nyc_gov_df['ZIP CODE'].astype(np.object)
nyc_gov_df = nyc_gov_df[['BOROUGH', 'NEIGHBORHOOD', 'ZIP CODE']]
nyc_gov_df = nyc_gov_df.replace([1], "manhattan")
nyc_gov_df = nyc_gov_df.replace([2], "bronx")
nyc_gov_df = nyc_gov_df.replace([3], "brooklyn")
nyc_gov_df = nyc_gov_df.replace([4], "queens")
nyc_gov_df = nyc_gov_df.replace([5], "staten_island")
nyc_gov_df.drop(nyc_gov_df[nyc_gov_df['ZIP CODE'].isna()].index, inplace=True)
nyc_gov_df['ZIP CODE'] = nyc_gov_df['ZIP CODE'].apply(lambda x : str(int(x)))
nyc_gov_df['ZIP CODE'] = nyc_gov_df['ZIP CODE'].astype(np.int64)
nyc_gov_df = nyc_gov_df.drop_duplicates()


# This is code to get the data into a nice matrix with borough included and the changes in price
#%%

zip_borough = pd.read_csv("data/real_estate/zip_to_borough.csv")
raw_df = pd.read_csv("data/real_estate/raw_real_estate_data.csv")
rent_df1 = pd.read_csv("data/real_estate/raw_rent_with_changes.csv")
sales_df = pd.read_csv("data/real_estate/raw_sales_with_changes.csv")
sales_df = sales_df.drop(sales_df[sales_df['price'].isna()].index)
sales_df = sales_df.drop(sales_df[~sales_df['event_description'].str.contains("rent")].index)
rent_df = pd.concat([rent_df1, sales_df], ignore_index=True)
rent_df.drop_duplicates(subset=['zillow_id', 'event_price', 'date'], inplace=True, ignore_index=True)

#%%

# rent_df.to_csv("raw_rent_with_changes.csv", index=False)

#%%

raw_df = raw_df[~(raw_df['status_type'] == 'FOR_RENT')]
raw_df.drop(raw_df[raw_df['zip_code'].isin(['Bay Terrace', 'Homecrest', 'Rochdale', 'North Riverdale', 'Clason Point', 'South Bronx', 'Old Town', 'Hunters Point', 'Ocean Hill'])].index, inplace=True)
raw_df.drop(raw_df[(raw_df['unformatted_price'] == 'None') | (raw_df['latitude'] == 'None') | (raw_df['longitude'] == 'None')].index, inplace=True)
raw_df['zillow_id'] = raw_df['zillow_id'].astype(np.int64)
raw_df['days_on_zillow'] = raw_df['days_on_zillow'].astype('int64')
a_date = datetime.date(2021, 4, 13)
raw_df['date'] = raw_df['days_on_zillow'].apply(lambda x : (a_date - datetime.timedelta(x)).strftime("%m/%d/%y"))
raw_df.drop(['days_on_zillow'], axis=1)


# raw_df = raw_df.drop(['status_text', 'home_status', 'days_on_zillow', 'time_on_zillow', 'address_state', 'address', 'latitude', 'longitude', 'tax', 'date_sold', 'variable_text', 'variable_type', 'price_reduction', 'price_change', 'price_change_date', 'rent_zestimate', 'zestimate', 'zillow_url', 'hdpdata_city', 'sold_price', 'price', 'price_per_sqrft', 'hdpdata_price', 'beds', 'baths', 'lot_size', 'year_built', 'address_zip_code'], axis=1)
raw_df = raw_df.drop(['status_text', 'home_status', 'days_on_zillow', 'time_on_zillow', 'address_state', 'address', 'latitude', 'longitude', 'tax', 'variable_text', 'variable_type', 'price_reduction', 'price_change', 'price_change_date', 'rent_zestimate', 'zestimate', 'zillow_url', 'hdpdata_city', 'sold_price', 'price', 'price_per_sqrft', 'hdpdata_price', 'beds', 'baths', 'lot_size', 'year_built', 'address_zip_code'], axis=1)
raw_df = raw_df.rename({'unformatted_price': 'price', 'hdpdata_bedrooms': 'beds', 'hdpdata_bathrooms': 'baths'}, axis=1)
raw_df['price'] = raw_df['price'].astype(np.int64)
raw_df['zip_code'] = raw_df['zip_code'].astype(np.int64)
raw_df['date'] = pd.to_datetime(raw_df['date'])
raw_df.drop_duplicates()

rent_df.drop(rent_df[(rent_df['date'].isnull())].index, inplace=True)
rent_df.drop(rent_df[rent_df['event_price'].str.contains("--")].index, inplace=True)
rent_df.drop(rent_df[rent_df['event_description'] == "Sold"].index, inplace=True)
rent_df = rent_df[['zillow_id', 'date', 'event_description', 'event_price']]
rent_df['event_price'] = rent_df['event_price'].apply(lambda x: x.split("(")[0].replace("$", "").replace(",", "") if "(" in x else x.replace("$", "").replace(",", "")).astype(np.int64)
rent_df.drop(rent_df[rent_df['event_price'] < 50_000].index, inplace=True)
rent_df['zillow_id'] = rent_df['zillow_id'].astype(np.int64)
rent_df = rent_df.rename({'event_price': 'price'}, axis=1)
rent_df['date'] = pd.to_datetime(rent_df['date'])
rent_df.drop_duplicates()

#%%

merged_df = rent_df.merge(raw_df, how="left", on=["zillow_id"]).drop(['date_y', 'price_y'], axis=1)
merged_df.drop_duplicates()
merged_df = merged_df.rename({'price_x': 'price', 'date_x': 'date'}, axis=1)

#%%
raw_df['event_description'] = "No price history"
result = pd.concat([merged_df, raw_df], ignore_index=True)
result.drop_duplicates(['zillow_id', 'date', 'price'], keep='first')
#%%

zip_to_borough = pd.read_csv("data/real_estate/zip_to_borough.csv")
zip_to_borough = zip_to_borough.rename({'ZIP CODE' : "zip_code"}, axis=1)
zip_to_borough.drop_duplicates(['zip_code'], keep="first")

#%% md
# if we want to maintain the number of columns we need to do the drop_duplicates using only the columns ['zillow_id', 'date', 'price']
# if not, for some reason we get duplicate rows in some places. (probably because the borough is different
#%%

result = result.merge(zip_to_borough, how="left", on=["zip_code"]).drop(['NEIGHBORHOOD'], axis=1).drop_duplicates()
result

#%%
result.drop_duplicates(['zillow_id', 'date', 'price'], inplace=True)
#%%
result.to_csv("fresh_sales_data.csv", index=False)