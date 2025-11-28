# Nigerian Exchange Rate Data Pipeline
# Project Start: November 26, 2025
# By: Abubakar Yahya Ibrahim

print("🚀 Ancient Braavos Data Engineering Portfolio")
print("=" * 50)
print("Day 1: Building my path from $2,500 to $50,000+")
print("Project 1: Nigerian Exchange Rate Pipeline")
print("=" * 50)

#import necessary libraries
import requests
import json
from datetime import datetime
import pandas as pd


#making the request
response = requests.get('https://v6.exchangerate-api.com/v6/e33490198a92a1c1bf75950d/pair/USD/NGN')
# checking for status
if response.status_code == 200:
  data = response.json()
  print('data succesfully retrieved !')
  print(json.dumps(data,indent=2))
else:
  print(f'Error: {response.status_code}')

# saving data in a better structured format
current_rate = data['conversion_rate']
exchange_data = {
    'date' : datetime.now().strftime('%Y-%m-%d'),
    'time' : datetime.now().strftime('%H:%M:%S'),
    'base_currency' : 'USD',
    'target_currency' : 'NGN',
    'exchange_rate' : current_rate,
    'source' : 'exchangerate-api.com'

}

#converting to pandas dataframe: more suited for analysis
df = pd.DataFrame(exchange_data, index = [0])
df.to_csv('usd_ngn_exchange_rate.csv')
print('\n Data saved to CSV!')
df
