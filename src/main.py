import argparse
import logging
import sys
from pyspark.sql import SparkSession
from data_loader import get_stocks_list, generate_dates_list, create_df_with_prices
from data_processing import find_greatest_increase, calculate_value_on_date
from typing import NoReturn


def main(input_path: str, output_path: str, api_key: str) -> NoReturn:
    logging.info("Input path to file: %s", input_path)
    logging.info("Target path: %s", output_path)
    logging.info("Target path: %s", api_key)
    spark = create_spark_session()

    start_date = "2022-03-01"
    end_date = "2023-12-31"

    symbols = get_stocks_list(spark, input_path)
    dates_list = generate_dates_list(start_date, end_date)

    df_with_prices = create_df_with_prices(spark, symbols, dates_list, api_key)

    find_greatest_increase(df_with_prices, output_path)
    calculate_value_on_date(df_with_prices, start_date, end_date, output_path)


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName('Batching') \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Process some paths.')
    parser.add_argument('input_path', type=str, help='Input path to file')
    parser.add_argument('output_path', type=str, help='Output path for the output')
    parser.add_argument('api_key', type=str, help='API key for Polyglot')

    args = parser.parse_args()

    try:
        main(args.input_path, args.output_path, args.api_key)
    except Exception as e:
        logging.error("Failed to complete processing due to an error: %s", e)
        sys.exit(1)
