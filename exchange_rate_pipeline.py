# Nigerian Exchange Rate Data Pipeline
# Project Start: November 26, 2025
# By: Abubakar Yahya Ibrahim

print("🚀 Babylon Data Engineering Portfolio")
print("=" * 50)
print("Day 1: Building my path from $2,500 to $50,000+")
print("Project 1: Nigerian Exchange Rate Pipeline")
print("=" * 50)

#import necessary libraries
import requests
import json
from datetime import datetime
import pandas as pd

"""
Babylon Exchange Rate Pipeline - Version 2.0
Multi-Currency Support with Error Handling
Author: Don Data (Abubakar Yahya Ibrahim)
Date: November 28, 2025
"""
#importing relevant libraries
import requests
import json
import pandas as pd
from datetime import datetime

#============================================== CONFIGURATION ==================================== ❌
API_KEY = 'e33490198a92a1c1bf75950d'
BASE_URL = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD'

#CURRENCIES IMPORTANT FOR NIGERIAN BUSINESSES
currencies = {
    'NGN':'Nigerian Naira',
    'EUR':'Euro',
    'GBP':'British Pounds',
    'CNY':'Chinese Yuan',
    'ZAR':'South African Rand',
    'GHS':'Ghanian Cedi'
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

  for code, name in currency_dict.items():
    rate = rates.get(code)

    if validate_fx(rate, code):
      records = {
          'timestamps': time_stamp,
          'date': datetime.now().strftime('%Y-%m-%d'),
          'time': datetime.now().strftime('%H:%M:%S'),
          'base_currnecy' : api_response.get('base-code','USD'),
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

  base = df['base_currency'].iloc[0]

  for _,row in dataframe.iterrows():
    rate_str = f"{row['exchange_rate']}".rjust(15)
    print(f" 1 {base} = {rate_str} {row['target_currency']} ({row['currency_name']})")

  print("="*60)
  print(f"Retrieved : {dataframe['date'].iloc[0]} at {dataframe['time'].iloc[0]} ")
  print(f"Source : {dataframe['source'].iloc[0]}")
  print("=" * 60 + "\n")


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

  return df


if __name__ == "__main__":
    result_df = main()
