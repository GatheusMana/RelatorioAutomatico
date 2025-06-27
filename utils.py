import pandas as pd
import requests

# Read Dataset From CSV to Dataframe
def read_dataset(path: str, interest_columns: list) -> pd.Dataframe:
  try:
    table = pd.read_csv(path)
  except Exception as e:
    print(f"An error has occurred: {e}")
  return table[interest_columns]

# Get Exchange Rate (INR -> currency_code)
def get_exchange_rate(currency_code: str, exchangerate_api_key: str) -> float:
  url = f"https://v6.exchangerate-api.com/v6/{exchangerate_api_key}/latest/INR"
  exchange_rates_list = requests.get(url).json()
  return exchange_rates_list["rates"][currency_code]

if __name__ == '__main__':
  interest_columns = ["Product", "Brand", "Price", "Product Code", "Inward Date", "Dispatch Date", "Quantity Sold"]
  tabela = read_dataset("data\mobile_sales_data.csv", interest_columns=interest_columns)
  tabela["Total Amount"] = tabela["Price"] * tabela["Quantity Sold"]
  print(tabela[["Brand", "Total Amount"]].groupby("Brand").sum())
  
  

