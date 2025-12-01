"""
Babylon Exchange Rate Pipeline - Version 2.0
Multi-Currency Support with Error Handling
Author: Don Data (Abubakar Yahya Ibrahim)
Date: december 1, 2025
"""
#importing relevant libraries
import requests
import json
import pandas as pd
from datetime import datetime
import  sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

#============================================== CONFIGURATION ==================================== ❌
API_KEY = 'e33490198a92a1c1bf75950d'
BASE_URL = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD'

#CURRENCIES IMPORTANT FOR NIGERIAN BUSINESSES
currencies = {
    'NGN':'Nigerian Naira',
    'CNY':'Chinese Yuan',
    'EUR':'Euro',
    'GBP':'British Pounds',
    'ZAR':'South African Rand'
}

#================================= funcions ========================
def fetch_fx_rates(api_key, base_currency : str = 'USD'):
  '''
  takes api keys and based currency as arguments. based currency is
  set to USD as default.
  fetches live rates via api with error handling
  api_key : is the api key required to access the server
  based_currency : base currency code, set to USD as default value
  '''
  url = f'https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}'
  try:
    print(f'Fetching rates for {base_currency} from API ...')
    response = requests.get(url, timeout= 10)
    response.raise_for_status()

    data = response.json()
    if data.get('result') == 'success':
      print(f"✅ Successfully retrieved {base_currency} rates")
      return data
    else:
      error_type = data.get('error_type','Unknown error') #get is dict method in python. data is a dict
      print(f'API error {error_type}')
      return None
  except requests.exceptions.Timeout:
    print('Request timed out')
    return None
  except requests.exceptions.ConnectionError:
    print('Connection error: check your internet connectivity')
    return None
  except requests.exceptions.RequestException as e:
    print(f'Request failed {str(e)}')
    return None

def validate_fx(rate: float, currency_code):
  '''
  Verifies whether the exchange rate is reasonably valid
  Arguments:-
  rate: the exchange rate value bieng validates, a float value
  currency_code: the currency code being validated
  returns: boolean value
  '''
  if rate is None:
    print(f"⚠️  {currency_code}: Rate is None")
    return None
  if rate <= 0:
    print(f"⚠️  {currency_code}: Rate is negative or zero")
    return False
  if rate > 50000:
    print(f"⚠️  {currency_code}: Rate seems unreasonably high ({rate})")
    return False
  return True

def extract_currency_data(api_response, currency_dict):
  '''
  extracts specific currency rate from the api response
  args:
  api_response: return value from fetch_fx_rate function
  curency_dict: a predefined dict of specific currencies
  returns: list of exchange rate record
  '''
  if not api_response:
    return [] #returns empty list if api_returns none
  record = []
  rates = api_response.get('conversion_rates', {})
  base_currency = api_response.get('base_code','USD')
  time_stamp = datetime.now()
  # Convert to ISO 8601 string
  time_stamp = time_stamp.isoformat()

  for code, name in currency_dict.items():
    rate = rates.get(code)

    if validate_fx(rate, code):
      records = {
          'timestamps': time_stamp,
          'date': datetime.now().strftime('%Y-%m-%d'),
          'time': datetime.now().strftime('%H:%M:%S'),
          'base_currency' : api_response.get('base-code','USD'),
          'target_currency': code,
          'currency_name' : name,
          'exchange_rate' : rate,
          'source': 'exchangerate-api.com',
          'api_update_time': api_response.get('time_last_update_utc','N/A')
    }
      record.append(records)
      print(f'✅ {code} 1 {base_currency} = {rate} {code}')

  return record


def save_to_csv(dataframe, file_name_prefix = 'exchange_rates'):
  '''
  takes return value from extract_currency_data function as a dictionary and
  turning it into a dataframe and saving it as csv file
  args:
  dictionary: dict returned from extract_currency_data
  file_name_prefix : will be attached for naming csv file
  '''
  if dataframe.empty:
    print('Empty records found')
    return None

  timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
  filename = f"{file_name_prefix}_{timestamp}_'.csv'"

  dataframe.to_csv(filename)
  print(f'\n Data saved to {filename}')
  print(f'Total records : {len(dataframe)}')

  return filename


def print_summary(dataframe):
  '''
  prints formatted summary of exchange rate
  args:
  dataframe: dataframe data
  '''
  if dataframe.empty:
    print('No data to display')
    return
  print('\n'+ '=' * 60)
  print("📊 EXCHANGE RATE SUMMARY - BABYLON PIPELINE")
  print("="*60)


  base = dataframe.loc[0,'base_currency']

  for _,row in dataframe.iterrows():
    rate_str = f"{row['exchange_rate']}".rjust(15)
    print(f" 1 {base} = {rate_str} {row['target_currency']} ({row['currency_name']})")

  print("="*60)
  print(f"Retrieved : {dataframe['date'].iloc[0]} at {dataframe['time'].iloc[0]} ")
  print(f"Source : {dataframe['source'].iloc[0]}")
  print("=" * 60 + "\n")

def create_table():
  '''
  creates table in the db by executing sql commands
  columns as available in the csv file.
  '''
  #connect to data base and create a db file if it doesn't exists
  conn = sqlite3.connect('babylon_exchange_rates.db')
  cursor = conn.cursor()
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS exchange_rates
      (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamps TEXT,
        date TEXT,
        time TEXT,
        base_currency TEXT,
        target_currency TEXT,
        currency_name TEXT,
        exchange_rate REAL,
        source TEXT,
        api_update_time TEXT,
        UNIQUE(date, target_currency)
      )
  ''')
  conn.commit()
  conn.close()
  print("✅ exchange_rates table succesfully created")
  return

#inserting exchange rate data into the table
def save_to_database(dataframe):
  '''
  inserts the exchange rates data in dataframe format into the db
  args:
  dataframe: file to be stored in table
  '''
  # SQL with INSERT OR IGNORE to skip duplicates
  try:
    #convert dataframe to list of tuples
    data_tuples = dataframe[[
            'timestamps','date', 'time', 'base_currency', 'target_currency',
            'currency_name', 'exchange_rate', 'source', 'api_update_time'
        ]].values.tolist()

    conn = sqlite3.connect('babylon_exchange_rates.db')
    cursor = conn.cursor()

    insert_sql = '''
        INSERT OR IGNORE INTO exchange_rates
        (timestamps, date, time, base_currency, target_currency, currency_name,
        exchange_rate, source, api_update_time)
        VALUES(?,?,?,?,?,?,?,?,?)
        '''
    conn.executemany(insert_sql, data_tuples)
    conn.commit()
    conn.close()
    rows_affected = cursor.rowcount
    print(f"✅ Database updated: {rows_affected} new records added")

  except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
  except Exception as e:
        print(f"❌ Error: {e}")
  return

def query():
  '''
  functions tries to pull data from db to check if previous ops were
  successful
  '''
  conn = sqlite3.connect('babylon_exchange_rates.db')

  query = 'SELECT * FROM exchange_rates ORDER BY date DESC'
  df = pd.read_sql_query(query, conn)

  conn.commit()
  conn.close()
  return df

# get data from database into dataframe
def pull_data():
  '''
  functions pulls data from db to check for for further processing
  successful.
  '''
  conn = sqlite3.connect('babylon_exchange_rates.db')

  query = 'SELECT * FROM exchange_rates ORDER BY date DESC'
  df = pd.read_sql_query(query, conn)

  conn.commit()
  conn.close()
  return df

def line_graph(data = pull_data()):
  '''
  takes returned data from pull_data function as input and isolate each currency pair.
  it then graphs the USDNGN pair.
  args:
  data: dataframe returned from pull_data
  '''
  #convert to similar date format
  data['data'] = pd.to_datetime(data['date'], format=('%Y-%m-%d'))
  #get unique currency codes
  currencies = data['target_currency'].unique()
  fig = make_subplots(
      rows=2, cols = 3, subplot_titles= [f'USD to {curr}' for curr in currencies],
      vertical_spacing = 0.12, horizontal_spacing = 0.10
      )

  # Add each currency to its subplot
  for i, currency in enumerate(currencies):
    row = (i // 3) + 1  # Which row (1, 2, or 3)
    col = (i % 3) + 1   # Which column (1, 2, or 3)
    
    # Filter data for this currency
    currency_data = data[data['target_currency'] == currency]
    
    # Add trace
    fig.add_trace(
        go.Scatter(
            x=currency_data['date'],
            y=currency_data['exchange_rate'],
            mode='lines+markers',
            name=currency,
            showlegend=False  # Turn off legend (titles show currency)
        ),
        row=row,
        col=col
    )
  fig.update_layout(
    height=900,  # Tall enough to see all
    title_text="Babylon Exchange Rate Tracker - All Currencies",
    showlegend=False
  )

  # Update all x-axes
  fig.update_xaxes(title_text="Date")

  # Update all y-axes
  fig.update_yaxes(title_text="Rate")

  fig.show()
  fig.write_html('babylon_exchange_rates.html')


  
  return

def main():
  """
  Main pipeline execution
  """
  print("\n🏛️  BABYLON EXCHANGE RATE PIPELINE - Version 2.0")
  print("=" * 60)
  print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
  print("=" * 60 + "\n")


  #Fetch data from API
  api_data = fetch_fx_rates(API_KEY)
  if not api_data:
    print("\n❌ Pipeline failed - could not retrieve data")
    return

  #Extract currency data
  print(f"\n📈 Extracting data for {len(currencies)} currencies... \n")
  exchange_records = extract_currency_data(api_data,currencies)
  if not exchange_records:
    print("\n❌ No valid exchange rates extracted")
    return
  #convert to dataframe
  df = pd.DataFrame(exchange_records, index = range(len(exchange_records)))

  #display summary
  print_summary(df)

  #save file to csv
  save_to_csv(df)

  #success message
  print("✅ Pipeline completed successfully!")
  print(f"🎯 Processed {len(df)} currency pairs\n")

  #create table
  create_table()

  #insert data into table
  save_to_database(df)

  #test whether succesful with select
  df = query()

  line_graph()

  return 


if __name__ == "__main__":
    result_df = main()
result_df
