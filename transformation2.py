"""## Extract Raw Data to S3 bucket"""

# Import libraries
import boto3
import pandas as pd
from geopy.geocoders import Nominatim
import dask.dataframe as dd
from datetime import datetime
from time import sleep
import random
from random import randint
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import requests
from requests.exceptions import RequestException
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import os
import logging
import re
import time
from tqdm import tqdm


# Configure logging
logging.basicConfig(filename='etl_transformation.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists('extract'):
    os.makedirs('extract')

def download_from_s3(bucket_name, files):
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
            key = file['key']
            filename = file['filename']
            s3.Bucket(bucket_name).download_file(Key=key, Filename=filename)
            print(f'{filename} downloaded')
        
    except Exception as e:
        print(f"An error occurred during file download: {e}")

# Define S3 bucket name and keys with corresponding filenames
bucket_name = 'ds4a-ppp-loan-raw'
files = [
    {'key': 'source1/rloans.csv', 'filename': 'extract/rloans.csv'},
    {'key': 'source2/rnaics.csv', 'filename': 'extract/rnaics.csv'},
    {'key': 'source3/rbalance_sheet.csv', 'filename': 'extract/rbalance_sheet.csv'},
    {'key': 'source4/rstock.csv', 'filename': 'extract/rstock.csv'}
]

# Download files from S3
download_from_s3(bucket_name, files)


def read_dataframes():
    try:
        rloan = dd.read_csv('extract/rloans.csv').compute()
        # Subset for 1500 records at random to remove potential bias
        rloan_sub = rloan.sample(n=1500).copy()
        print('rloan.csv processed')

        rnaics = pd.read_csv('extract/rnaics.csv')
        rbalance_sheet = pd.read_csv('extract/rbalance_sheet.csv')
        rstock = pd.read_csv('extract/rstock.csv')
        print('Extraction completed')

        return rloan_sub, rnaics, rbalance_sheet, rstock
    except Exception as e:
        print(f"An error occurred during DataFrame reading: {e}")

# Read the files into DataFrames
rloan_sub, rnaics, rbalance_sheet, rstock = read_dataframes()


"""## Data Cleaning

### Check for null values
"""
def check_for_null(dataframes):
    for i, df in enumerate(dataframes):
        null_sum = df.isna().sum().sum()
        print(f"DataFrame {i+1}: Sum of nulls: {null_sum}")
    print(rloan_sub.isnull().sum())
    print(rnaics.isnull().sum())

"""### Investigate null values for each dataframe

#### rloan
"""

"""Instead of removing records, a dummy value of "999999" can be set for missing NAICS codes. 
Analysts should be made aware of to filter out this value during analysis."""

def fill_naics_code(df):
    try: 
        df['NAICSCode'] = df['NAICSCode'].fillna(999999)
        return df 
    except Exception as e:
        logging.error(f"An error occurred in fill_naics_code: {e}")


"""For borrowers who have not been forgiven, forgiven amount is zero and there is no entry for ForgivenessDate; 
these values need to be explicitly set to zero for amount and the date for data extraction instead of remaining as missing/null."""

# Set forgiveness amount to zero and date as a current date indicating to filter out for unforgiven loans. 
def fill_forgiveness_data(df):
    try: 
        df['ForgivenessAmount'] = df['ForgivenessAmount'].fillna(0)
        df['ForgivenessDate'] = df['ForgivenessDate'].fillna('06/24/2023')
        return df
    except Exception as e:
        logging.error(f"An error occurred in fill_forgiveness_data: {e}")
        
"""In this random slice of loan data, there was minimal category of missing or null value that has a huge impact on analysis in which records needs to be removed. 
For example, since geopy is needed to calculate for longitude, latitude, and county information, missing borrower location variables will affect processing, 
so any records will be removed from further analysis on the assumption that the record represent <5% of the analysis sample and should have no effect on the data.

"""

"""def remove_location_records(df):
    # Assuming missing location records represent <5% of the analysis sample, remove them
    try:
        df = df.dropna(subset=['BorrowerLocation'])

    # Reset index after removing rows
        df = df.reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"An error occurred in remove_location_records: {e}")
"""

"""
#### rnaics
"""

"""Appears that unnamed:2 is an empty column due to how the csv was originally saved as a source with an extra column containing no relevant data. 
Removing this column is assumed to have no impact on the data. In addition, the first row after the header has null values and will also be removed."""

def remove_null_records(df):
    try:
        print("Before dropping columns with all NaN values:")
        print(df.isnull().sum())  # Print count of null values in each column
        
        df = df.drop('Unnamed: 2', axis=1)  # Remove the 'Unnamed: 2' column
        
        print("Before dropping rows with any NaN values:")
        print(df.isnull().sum())  # Print count of null values in each column
        df = df.dropna()  # Remove rows with any NaN values in the remaining columns
        
        df = df.reset_index(drop=True)
        print("After removing null records:")
        print(df.isnull().sum())  # Print count of null values in each column

        return df
    except Exception as e:
        logging.error(f"An error occurred in remove_null_records: {e}")

"""#### rbalance_sheet"""

"""Through research, Net debt is typically calculated as the difference between a company's total debt and its cash and cash equivalents. 
Thus, from the balance sheet, NetDebt can be calculated by subtracting CashAndCashEquivalents from TotalDebt. 
Confirmed calculations manually with known NetDebt entries. Part of this process is to go back and retrieve additional column for balance sheet data to reupload to s3."""

def calculate_net_debt(df):
    for index, row in df.iterrows(): # Loop to iterate over each row and find missing NetDebt value
        if pd.isnull(row['NetDebt']):
            net_debt = row['TotalDebt'] - row['CashAndCashEquivalents'] # Calculate using TotalDebt minus CashAndCashEquivalents
            df.at[index, 'NetDebt'] = net_debt # Add the calculated net debt value to the missing row
    print(df.isnull().sum())  # Print count of null values in each column
    return df


"""Interestingly, the null net debt are negative values. Treasury number and preferred shares number do not have any possible extrapolation with current data. 
These values have no impact on data processing and can be ignored.
"""

# Run call functions 
dataframes = [rloan_sub, rnaics, rbalance_sheet, rstock]
try:
    check_for_null(dataframes)
    fill_naics_code(rloan_sub)
    fill_forgiveness_data(rloan_sub)
    #rloan_sub = remove_location_records(rloan_sub)
    rnaics = remove_null_records(rnaics)
    rbalance_sheet = calculate_net_debt(rbalance_sheet)

except Exception as e:
    logging.error(f"An error occurred in the main data processing: {e}")


## Check for duplicates


# Use loop to check for dups

def check_duplicates(dataframes):
    for i, df in enumerate(dataframes):
        duplicates_sum = df.duplicated().sum().sum()
        print(f"DataFrame {i+1}: Sum of duplicates: {duplicates_sum}")

check_duplicates(dataframes)
"""No further processing needed."""

"""

# Validating Data

### Ensure the correct date range and datetype for date values
"""

# Verify date range for stocks data


# Confirm range with if statement
def validate_date_range(df, start_date, end_date):
    try:
        is_within = (df['Date'] >= start_date) & (df['Date'] <= end_date)
        if is_within.all():
            print("All dates are within the specified range.")
        else:
            print("Not all dates are within the specified range.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Define the date boundaries
start_date = '2020-03-01'
end_date = '2023-12-31'
validate_date_range(rstock, start_date, end_date)

"""### Check to make sure data type matches that of data procurement and data modeling"""

# Format datetime columns
# Format the date column in the DataFrame to datetime type
rloan_sub['DateApproved'] = pd.to_datetime(rloan_sub['DateApproved'])
rloan_sub['ForgivenessDate'] = pd.to_datetime(rloan_sub['ForgivenessDate'])
rbalance_sheet['asOfDate'] = pd.to_datetime(rbalance_sheet['asOfDate'])


# Validate data types

# Loop to iterate through each column and check dtypes. Print statement notify errors.
def validate_data_types(df, expected_dtypes):
    try:
        for col, expected_dtype in expected_dtypes.items():
            if col in df.columns:
                if df[col].dtype == expected_dtype:
                    print(f"Column '{col}' has the expected data type.")
                else:
                    print(f"Column '{col}' DOES NOT have the expected data type.")
            else:
                print(f"Column '{col}' DOES NOT exist in the DataFrame.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Define dict of expected data types based on data schema
expected_dtypes_rloan_sub = {
    'LoanNumber': 'int',
    'DateApproved': 'datetime64[ns]',
    'BorrowerName': 'object',
    'BorrowerAddress': 'object',
    'BorrowerCity': 'object',
    'BorrowerState': 'object',
    'BorrowerZip': 'object',
    'InitialApprovalAmount': 'float64',
    'NAICSCode': 'float64',
    'OriginatingLender': 'object',
    'ForgivenessAmount': 'float64',
    'ForgivenessDate': 'datetime64[ns]',
    'JobsReported': 'float64'
}
validate_data_types(rloan_sub, expected_dtypes_rloan_sub)

expected_dtypes_rnaics = {
    '2017 NAICS Code': 'float64',
    '2017 NAICS Title': 'object'
}
validate_data_types(rnaics, expected_dtypes_rnaics)

expected_dtypes_rbalance_sheet = {
    'symbol': 'object',
    'asOfDate': 'datetime64[ns]',
    'CashAndCashEquivalents': 'float64',
    'CommonStockEquity': 'float64',
    'InvestedCapital': 'float64',
    'NetDebt': 'float64',
    'NetTangibleAssets': 'float64',
    'OrdinarySharesNumber': 'float64',
    'PreferredSharesNumber': 'float64',
    'PreferredStockEquity': 'float64',
    'ShareIssued': 'float64',
    'TangibleBookValue': 'float64',
    'TotalAssets': 'float64',
    'TotalCapitalization': 'float64',
    'TotalDebt': 'float64',
    'TotalEquityGrossMinorityInterest': 'float64',
    'TotalLiabilitiesNetMinorityInterest': 'float64',
    'TreasurySharesNumber': 'float64'
}
validate_data_types(rbalance_sheet, expected_dtypes_rbalance_sheet)

# Convert LoanNumber to the correct data type
def convert_loan_number(df):
    try:
        df['LoanNumber'] = df['LoanNumber'].astype(str).str.strip()
        df['LoanNumber'] = df['LoanNumber'].apply(lambda x: int(float(x)))
        return df
    except Exception as e:
        logging.error(f"An error occurred: {e}")

rloan_sub = convert_loan_number(rloan_sub)
"""# Transforming Data

### Transform stock df to match schema
"""

# Use the 'melt()' function to transform the DataFrame
def transform_stock_data(df):
    melt_df = pd.melt(df, id_vars=['Date'], var_name='LenderID', value_name='StockPrice')
    return melt_df
# Check melt
melt_rstock = transform_stock_data(rstock)


"""#### Create source 5 Longitude, Latitude, and County data"""

# Pre-process borrower address to remove suite and other outliers that geopy cannot code for


# Function to process the address and extract the part up to the street type
def process_address(address):
    street_types = [
        "Street", "St.", "Parkway", "Pkwy.", "Boulevard", "Blvd.", "Road", "Rd.",
        "Avenue", "Ave.", "Lane", "Ln", "Court", "Ct", "Circle", "Cir", "Drive", "Dr",
        "Terrace", "Ter", "Highway", "Hwy", "Place", "Pl", "Square", "Sq", "Way", "Loop",
        "Alley", "Route", "Expressway", "Expy", "Freeway", "Fwy", "Bridge", "Br"
    ]
    address = address.strip()
    for street_type in street_types:
        pattern = r'^(.*?\b{}\b)(?:\b.*?$|$)'.format(street_type)
        match = re.search(pattern, address, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return address

# Apply the function to the 'BorrowerAddress' column and save the cleaned addresses in a new column

def create_cleaned_address(df, address_column, cleaned_address_column):
    df[cleaned_address_column] = df[address_column].apply(process_address)
    return df

rloan_sub = create_cleaned_address(rloan_sub, 'BorrowerAddress', 'BorrowerAddress_Cleaned')


# Create full address column
rloan_sub['Full_Address'] = rloan_sub['BorrowerAddress_Cleaned'] + ', ' + rloan_sub['BorrowerCity'] + ', ' + rloan_sub['BorrowerState'] + ' ' + rloan_sub['BorrowerZip']

# Function found on Stack Overflow to help avoid timeout error for API call constraints
def reverse_geocode(geolocator, latlon, sleep_sec):
    try:
        return geolocator.reverse(latlon)
    except GeocoderTimedOut:
        print('TIMED OUT: GeocoderTimedOut: Retrying...')
        time.sleep(randint(1 * 100, sleep_sec * 100) / 100)
        return reverse_geocode(geolocator, latlon, sleep_sec)
    except GeocoderServiceError as e:
        print('CONNECTION REFUSED: GeocoderServiceError encountered.')
        print(e)
        return None
    except Exception as e:
        print('ERROR: Terminating due to exception {}'.format(e))
        return None

# Add empty columns
rloan_sub['Latitude'] = None
rloan_sub['Longitude'] = None
rloan_sub['County'] = None

# Find Longitude, Latitude, and County with geopy
geolocator = Nominatim(user_agent="test_agent")

total_records = len(rloan_sub)

# Use tqdm to create a progress bar
for index, row in tqdm(rloan_sub.iterrows(), total=total_records, desc="Processing records"):
    try:
        location = geolocator.geocode(row['Full_Address'], timeout=10)
        if location is not None:
            rloan_sub.at[index, 'Latitude'] = location.latitude
            rloan_sub.at[index, 'Longitude'] = location.longitude
            loc_str = str(geolocator.reverse((location.latitude, location.longitude), zoom=8))
            rloan_sub.at[index, 'County'] = loc_str.split(',')[0]
    except AttributeError:
        pass

    time.sleep(1)  # Add 1-second delay between requests to avoid timeout error

print("Processing completed for all records.")

# Add Year column to match data schema
rbalance_sheet['Year'] = rbalance_sheet['asOfDate'].dt.year

# Create Lender Dataframe
def create_lender_df():
    lender = {
        'LenderID': ['BAC', 'JPM', 'WFC'],
        'LenderName': ['Bank of America, National Association', 'JPMorgan Chase Bank, National Association', 'Wells Fargo Bank, National Association']
    }
    return pd.DataFrame(lender)

plender = create_lender_df()
plender.to_csv('plender.csv', index = False)

# Create new column for Loan Status
def add_loan_status(df):
    try:
        loan_status = []
        for index, row in df.iterrows():
            if row['ForgivenessAmount'] == row['InitialApprovalAmount']:
                loan_status.append("Forgiven")
            elif row['ForgivenessAmount'] > row['InitialApprovalAmount']:
                loan_status.append("Forgiven")
            else:
                loan_status.append("Not Forgiven")
        df['LoanStatus'] = loan_status
        return df
    except Exception as e:
        logging.error(f"An error occurred: {e}")

rloan_sub = add_loan_status(rloan_sub)


"""# Primary ID"""

# Make unique ID columns
def add_primary_id(df):
    df['StockID'] = df.reset_index().index + 1
    df['BorrowerID'] = df.reset_index().index + 1
    return df

melt_rstock = add_primary_id(melt_rstock)
rloan_sub = add_primary_id(rloan_sub)

def add_balance_id(df):
    df['ID'] = df['symbol'] + '_' + df['Year'].astype(str)
    return df

rbalance_sheet = add_balance_id(rbalance_sheet)

# Uploading Processed CSV

# Save processed dataframes as csv
rnaics.to_csv('pnaics.csv', index=False)
rbalance_sheet.to_csv('pbalance_sheet.csv', index=False)
melt_rstock.to_csv('pstock.csv', index=False)

rloan_sub.to_csv('ploan_sub.csv', index=False)  # Remove index while saving
rloan_sub.to_csv('p2loan_sub.csv')  # Save index as individual column


def upload_to_s3(files, bucket_name):
    try:
        load_dotenv('processed.env')  # Load the environmental file
        access_key_id = os.getenv('P_API_KEY')
        secret_access_key = os.getenv('P_SECRET_KEY')

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


# Define the files to upload
files = [
    {'filename': 'pnaics.csv', 'key': 'source2/pnaics.csv'},
    {'filename': 'pbalance_sheet.csv', 'key': 'source3/pbalance_sheet.csv'},
    {'filename': 'pstock.csv', 'key': 'source4/pstock.csv'},
    {'filename': 'ploan_sub.csv', 'key': 'source1/ploan_sub.csv'},
    {'filename': 'p2loan_sub.csv', 'key': 'source1/p2loan_sub.csv'},
    {'filename': 'plender.csv', 'key': 'source1/plender.csv'}
]

bucket_name = 'ds4a-ppp-loan-processed'
upload_to_s3(files, bucket_name)