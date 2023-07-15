CREATE OR REPLACE PROCEDURE ppp.sp_trunc_load_data()
 LANGUAGE plpgsql
AS $$
BEGIN

--truncate 
TRUNCATE TABLE ppp.fact_stocks;
TRUNCATE TABLE ppp.fact_loans;
TRUNCATE TABLE ppp.dim_balancesheets;
TRUNCATE TABLE ppp.dim_borrowers;
TRUNCATE TABLE ppp.dim_lenders;
TRUNCATE TABLE ppp.dim_naics;

RAISE NOTICE 'the tables have been truncated; data loading is starting';

--load data from S3

COPY team16.ppp.dim_naics (naics_code, naics_title) FROM 's3://ds4a-ppp-loan-transformed/dim_naics.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

COPY team16.ppp.dim_lenders (lender_symbol, lender_name) FROM 's3://ds4a-ppp-loan-transformed/dim_lender.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

COPY team16.ppp.dim_borrowers (borrower_id, borrower_name, borrower_address, borrower_city, borrower_state, borrower_zip, borrower_latitude, borrower_longitude, borrower_county) FROM 's3://ds4a-ppp-loan-transformed/dim_borrower.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

COPY team16.ppp.dim_balancesheets (lender_symbol, common_stock_equity, net_debt, invested_capital, net_tangible_assets, ordinary_shares_number, preferred_shares_number, preferred_stock_equity, share_issued, tangible_book_value, total_assets, total_capitalization, total_debt, total_equity_gross_minority_interest, total_liabilities_net_minority_interest, treasury_shares_number, year, balancesheet_id) FROM 's3://ds4a-ppp-loan-transformed/dim_balancesheets.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

COPY team16.ppp.fact_stocks (stock_date, lender_symbol, stock_price, stock_id) FROM 's3://ds4a-ppp-loan-transformed/fact_stocks.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

COPY team16.ppp.fact_loans (loan_number, date_approved, initial_approval_amount, naics_code, forgiveness_amount, forgiveness_date, jobs_reported, borrower_id, forgiveness_status, lender_symbol) FROM 's3://ds4a-ppp-loan-transformed/fact_loans.csv' IAM_ROLE 'custom-redshift-role' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2';

END;
$$