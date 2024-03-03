from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as sum_
from datetime import date


def write_to_file(output_file: str, message: str) -> None:
    """Utility function to write a message to a file."""
    with open(output_file, 'a') as file:
        file.write(message + "\n")


def find_greatest_increase(df: DataFrame, output_file: str) -> None:
    """Find and print the stock with the greatest percentage increase."""
    df_change = df.withColumn("percentage_change",
                              ((col("closing_price") - col("opening_price")) / col("opening_price")) * 100)

    # Find the stock with the greatest percentage increase
    greatest_increase = df_change.orderBy(col("percentage_change").desc()).first()

    output = f"The stock with the greatest relative increase is {greatest_increase['symbol']} with a change of {greatest_increase['percentage_change']:.2f}%."
    # Save the result
    write_to_file(output_file, output)


def calculate_value_on_date(df: DataFrame, start_date: date, end_date: date, output_file: str) -> None:
    """Calculate and print the total value of an equal investment in each stock over a period."""
    # Filter for the first date to get the opening prices
    first_date_prices = df.filter(df['date'] == start_date)

    # Calculate the number of shares bought with $10,000 for each stock
    first_date_prices = first_date_prices.withColumn("shares_bought", 10000 / col("opening_price")) \
        .withColumnRenamed("closing_price", "closing_price_first_date")

    # Join this with the closing prices on the last date
    # Assuming 'df' is already sorted by date, or you can ensure to sort it
    last_date_prices = df.filter(df['date'] == end_date) \
        .select("symbol", "closing_price") \
        .withColumnRenamed("closing_price", "closing_price_last_date")

    # Assuming 'symbol' is unique enough for this operation (i.e., one entry per symbol on the last date)
    investment_value = first_date_prices.join(last_date_prices, "symbol")

    # Calculate the value at the end of the period for each investment
    investment_value = investment_value.withColumn("end_value", col("shares_bought") * col("closing_price_last_date"))

    # Sum up the end values to get the total value of the investment
    total_end_value = investment_value.select(sum_("end_value")).collect()[0][0]

    output = f"Total value of the investment at the end of the period: ${total_end_value:,.2f}\n"
    write_to_file(output_file, output)
