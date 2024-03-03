import requests
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def get_stock_prices(symbol, api_key, date):
    url = f"https://api.polygon.io/v1/open-close/{symbol}/{date}?adjusted=true&apiKey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for 4XX or 5XX responses

        data = response.json()
        selected_data = (data['symbol'], data['from'], float(data['open']), float(data['close']))
        return selected_data

    except requests.exceptions.HTTPError as http_err:
        # Specific handling for HTTP errors
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
    except KeyError as key_err:
        # Handling missing keys in the JSON data
        print(f"Key error: {key_err}")
    except Exception as err:
        # Handling other unforeseen errors
        print(f"An error occurred: {err}")


def get_stocks_list(spark: SparkSession, input_path: str) -> list:
    df = spark.read.csv(input_path, header=True)
    # Collect the s&p symbols into a list
    symbols = df.select("symbol").rdd.flatMap(lambda x: x).collect()
    return symbols


def create_df_with_prices(spark: SparkSession, symbols: list, dates_list: list, api_key: str) -> DataFrame:
    stock_prices = []

    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), True),
        StructField("opening_price", FloatType(), True),
        StructField("closing_price", FloatType(), True)
    ])
    # Fetch stock prices for each symbol
    for symbol in symbols:
        for d in dates_list:
            result = get_stock_prices(symbol, api_key, d)
            if result is not None:
                stock_prices.append(result)

    # Create the DataFrame
    return spark.createDataFrame(stock_prices, schema)


def generate_dates_list(start: str, end: str) -> list:
    # Extract just the date part
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()

    # Generate the list of dates
    current_date = start_date
    date_list = []

    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)

    return date_list