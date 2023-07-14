# Import libraries
import boto3
import pandas as pd
from yahooquery import Ticker
import yfinance as yf
import dask.dataframe as dd
import requests
from requests.exceptions import RequestException
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(filename='etl_storage.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def get_loans(urls, cols):
    try:
        # Read csv files into DataFrame with dask
        ddf_all = dd.read_csv(urls, dtype={
            'LoanNumber': 'float64',
            'DateApproved': 'object',
            'NAICSCode': 'float64',
            'JobsReported': 'float64',
            'NonProfit': 'object',
            'UndisbursedAmount': 'float64',
            'BorrowerState': 'object',
            'FranchiseName': 'object',
            'OriginatingLenderLocationID': 'float64',
            'SBAOfficeCode': 'float64',
            'ServicingLenderLocationID': 'float64'})

        # Filter for interested lenders discovered during Data Procurement to reduce data volume given AWS storage constraints
        print("Filtering data...")
        ddf_filtered = ddf_all.query(
            "OriginatingLender in ['Bank of America, National Association', 'JPMorgan Chase Bank, National Association', 'Wells Fargo Bank, National Association']")

        # Output to DataFrame
        print("Computing filtered DataFrame...")
        dfiltered = ddf_filtered[cols].compute()

        # Save as csv
        print("Saving as CSV...")
        dfiltered.to_csv('rloans.csv', index=False)

    except RequestException as e:
        logging.error(f"An error occurred during API request: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# URLs to all PPP loan source files
urls_all = [
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/4b3c3e7a-1286-4883-b857-d37058f9693c/download/public_150k_plus_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/c95195f6-0af6-4b84-8c65-e7cd6b940cc2/download/public_up_to_150k_1_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/f7b72681-f697-48f4-9931-71c0f3422ec4/download/public_up_to_150k_2_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/eaa51a51-ef19-4c22-affe-61ede7253c6f/download/public_up_to_150k_3_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/b4ec101e-ad78-4a25-a058-ab03b049766b/download/public_up_to_150k_4_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/ea284b67-d0b7-4e65-bc48-663e9bb6dac1/download/public_up_to_150k_5_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/ef56afe8-08f8-4bfa-8a58-29690f5baae0/download/public_up_to_150k_6_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/81f1e2be-28a2-4854-bfe1-1e0d408f9fd0/download/public_up_to_150k_7_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/6ada73a2-8176-4e71-8689-30490d9f8a2f/download/public_up_to_150k_8_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/985f0c28-e799-4940-94a9-96a7c5c604a6/download/public_up_to_150k_9_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/3487edaa-92b3-47f4-b147-06fd6d79f786/download/public_up_to_150k_10_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/fdbaf355-ebfc-4ed3-a85c-f8b912232cca/download/public_up_to_150k_11_230630.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/d548c61b-f189-4673-8bbb-553fbc9a7cf9/download/public_up_to_150k_12_230630.csv'
    ]

# Columns of interest for PPL loan
cols = [
    'LoanNumber',
    'DateApproved',
    'BorrowerName',
    'BorrowerAddress',
    'BorrowerCity',
    'BorrowerState',
    'BorrowerZip',
    'NAICSCode',
    'OriginatingLender',
    'InitialApprovalAmount',
    'ForgivenessAmount',
    'ForgivenessDate',
    'JobsReported'
    ]


def get_naics(url):
    try:
        df_naics = pd.read_excel(url)
        df_naics.to_csv('rnaics.csv', index=False)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# URL for NAICS data
url_naics = 'https://www.census.gov/naics/2017NAICS/6-digit_2017_Codes.xlsx'

# Fetch and save NAICS data
get_naics(url_naics)

def get_balance_sheet(symbols, filename):
    try:
        balance_sheets = symbols.balance_sheet(frequency='a')

        # Reset the index to convert symbol index level to a column
        balance_sheets = balance_sheets.reset_index()

        # Limit balance sheet to only relevant columns
        balance_sheets = balance_sheets[['symbol', 'asOfDate', 'CashAndCashEquivalents',
                                         'CommonStockEquity',
                                         'InvestedCapital',
                                         'NetDebt',
                                         'NetTangibleAssets',
                                         'OrdinarySharesNumber',
                                         'PreferredSharesNumber',
                                         'PreferredStockEquity',
                                         'ShareIssued',
                                         'TangibleBookValue',
                                         'TotalAssets',
                                         'TotalCapitalization',
                                         'TotalDebt',
                                         'TotalEquityGrossMinorityInterest',
                                         'TotalLiabilitiesNetMinorityInterest',
                                         'TreasurySharesNumber']]
            
        # Save DataFrame as a CSV file
        balance_sheets.to_csv(filename, index=False)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


# Fetch and save balance sheet data given symbols
symbols = Ticker('bac, jpm, wfc')
get_balance_sheet(symbols, 'rbalance_sheet.csv')

def get_stock(tickers):
    try:
        # Extract data into dataframe
        df_stock = yf.download(tickers, start='2020-03-01', end='2020-12-31')['Adj Close']
        df_stock.to_csv('rstock.csv')
    except HTTPError as e:
        logging.error(f"An HTTP error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Tickers for stock data
tickers = ['BAC', 'JPM', 'WFC']

# Fetch and save stock data
get_stock(tickers)

# Fetch and save loans data
get_loans(urls_all, cols)

# Configure logging
logging.basicConfig(filename='raw_storage.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_to_s3(files, bucket_name):
    try:
        load_dotenv('raw.env')  # Load the environmental file
        access_key_id = os.getenv('R_API_KEY')
        secret_access_key = os.getenv('R_SECRET_KEY')

        s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-2',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )

        for file in files:
            filename = file['filename']
            key = file['key']
            s3.Bucket(bucket_name).upload_file(Filename=filename, Key=key)
            print(f'{filename} uploaded')

    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Files to upload to S3
bucket_name = 'ds4a-ppp-loan-raw'
files = [
    {'filename': 'rnaics.csv', 'key': 'source2/rnaics.csv'},
    {'filename': 'rbalance_sheet.csv', 'key': 'source3/rbalance_sheet.csv'},
    {'filename': 'rstock.csv', 'key': 'source4/rstock.csv'},
    {'filename': 'rloans.csv', 'key': 'source1/rloans.csv'}
    
]

# Upload files to S3 bucket
upload_to_s3(files, bucket_name)