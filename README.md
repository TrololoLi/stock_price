
# Spark Data Processing Application

The application extracts stocks opening and closing prices for provided list of companies, calculates relative increase in price and value on date

## Getting Started

### Prerequisites

- Apache Spark
- Python 3.x
- Required Python packages as listed in `requirements.txt`
- Create account with Polygon https://polygon.io/docs/stocks/getting-started
- CSV file with companies that you are interested with companies symbols

### Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/TrololoLi/met_objects_batching.git
   cd met_objects_batching
   ```

2. **Set Up a Python Virtual Environment (Optional but Recommended)**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Required Python Packages**

   ```bash
   pip install -r requirements.txt
   ```

### Usage

To run the application, use the following `spark-submit` command, specifying the path to your input dataset and the desired output path:
    
```bash
spark-submit src/main.py <input_path> <output_path> <api_key>
```
example:
```bash
 spark-submit src/main.py "/Users/tatiana/Downloads/stocks.csv" "/Users/tatiana/Downloads/result.txt" "iKOsELZITFEqwusyGb0hVKjo"
```

#### Arguments

- `input_path`: Path to the input dataset csv file.
- `output_path`: Folder where the processed data will be saved.
- `api_key`:  API key that provides access to Polyglot