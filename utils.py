import pandas as pd
import requests
from typing import Optional

# Read Dataset From CSV to Dataframe 
def read_dataset(path: str, interest_columns: list) -> Optional[pd.DataFrame]:
  try:
    if not path:
        raise FileNotFoundError("Path cannot be empty.")
    if not interest_columns:
        raise ValueError("Interest Columns cannot be empty")

    df = pd.read_csv(path.strip())
    missing_columns = [col for col in interest_columns if col not in df]

    if missing_columns:
        raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
    
    return df[interest_columns]
  
  except FileNotFoundError as e:
    print(e)
    return None
  except ValueError as e:
    print(e)
    return None  
  except Exception as e:
    print(f"Unexpected error: {e}")
    return pd.DataFrame()

# Get Exchange Rate (INR -> currency_code)
def get_exchange_rate(api_key: str, currency_code: str = "USD") -> Optional[float]:
    if currency_code == "INR":
        return 1.0

    try:
        url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/INR"
        response = requests.get(url).json()
        rates = response.get("conversion_rates", {})
        exchange_rate = rates.get(currency_code)

        if not exchange_rate:
            raise ValueError(f"Currency code '{currency_code}' not found in API response.")

        return float(exchange_rate)

    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

# Update currency in dataframe
def update_currency(df: pd.DataFrame, exchange_rate: float, currency_columns: list = None) -> Optional[pd.DataFrame]:
    if currency_columns is None:
        currency_columns = ["Price"]
    try:
        if df.empty:
            raise ValueError("Table cannot be empty")

        df_updated = df.copy()
        if not isinstance(exchange_rate, (int, float)):
            raise TypeError(f"Exchange rate is not a number.")

        missing_columns = [col for col in currency_columns if col not in df_updated]
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
        for column in currency_columns:
            df_updated[column] = df_updated[column] * exchange_rate
        return df_updated
    except TypeError as e:
       print(e)
       return df
    except ValueError as e:
       print(e)
       return None
    except Exception as e:
       print(f"Unexpected error: {e}")
       return None

#Create a column named Total Amount for analisis
def set_metrics_table(df: pd.DataFrame, required_columns: list = None) -> Optional[pd.DataFrame]:
    if required_columns is None:
        required_columns = ["Price", "Quantity Sold", "Dispacth Date"]
    try:
        if df.empty:
            raise ValueError("Table cannot be empty")
        
        required_columns = ["Price", "Quantity Sold", "Dispacth Date"]
        missing_columns = [col for col in required_columns if col not in df]
        
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
        
        df["Total Amount"] = df["Price"] * df["Quantity Sold"]

        return df
    
    except ValueError as e:
       print(e)
       return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

#Get Brand Sales (Use best for best brands (True) or worst brands (False))
def get_brand_sales_table(df: pd.DataFrame, best: bool = True, top_number: int = 10, required_columns: list = None) -> pd.DataFrame:
    if required_columns is None:
        required_columns = ["Brand", "Total Amount"]
    try:
        if df.empty:
            raise ValueError("Table cannot be empty")

        missing_columns = [col for col in required_columns if col not in df]
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
        
        total_brands = len(df["Brand"].unique())
        if top_number > total_brands:
            print(f"Total brands is less then {top_number} -> changing top number to fit in the table")
            top_number = min(top_number, total_brands)
            
        
        brand_sales = df.groupby("Brand")["Total Amount"].sum().reset_index()
        brand_sales = brand_sales.sort_values("Total Amount", ascending=not best)
        
        return brand_sales.head(top_number)
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    
#Get Product Sales (Use best for best products (True) or worst products (False))
def get_product_sales_table(df: pd.DataFrame, best: bool = True, top_number: int = 10, required_columns: list = None) -> Optional[pd.DataFrame]:
    if required_columns is None:
        required_columns = ["Product","Product Code", "Brand", "Price", "Total Amount"]
    try:
        if df.empty:
            raise ValueError("Table cannot be empty")
        
        missing_columns = [col for col in required_columns if col not in df]
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
        
        total_products = len(df["Product Code"].unique())
        if top_number > total_products:
            print(f"Total products is less then {top_number} -> changing top number to fit in the table")
            top_number = min(top_number, total_products)

        product_sales = df.groupby(["Product Code", "Product", "Brand"], as_index=False)["Total Amount"].sum()
        product_sales = product_sales.sort_values("Total Amount", ascending=not best)

        return product_sales.head(top_number)
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

#Get Sales X Month relation
def get_month_sales_table(df: pd.DataFrame, date_col="Dispatch Date"):
    try:
        if df.empty:
            raise ValueError("Table cannot be empty")
        
        monthly_sales = df.copy()

        if date_col not in df.columns:
            raise ValueError(f"Missing column in DataFrame: {date_col}")

        monthly_sales[date_col] = pd.to_datetime(monthly_sales[date_col], errors='coerce'  )
        monthly_sales["Year"] = monthly_sales[date_col].dt.year
        monthly_sales["Month"] = monthly_sales[date_col].dt.month_name()

        monthly_sales = monthly_sales.groupby(['Year', 'Month'])['Total Amount'].sum().reset_index()
        monthly_sales = monthly_sales.sort_values(['Year', 'Month'])
        return monthly_sales
        
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

