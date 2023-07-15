CREATE OR REPLACE PROCEDURE ppp.sp_create_tables()
 LANGUAGE plpgsql
AS $$
BEGIN
CREATE TABLE ppp.dim_naics (
    naics_code INTEGER NOT NULL,
    naics_title CHARACTER VARYING (200),
    PRIMARY KEY (naics_code)
) ENCODE AUTO;

CREATE TABLE ppp.dim_lenders (
    lender_symbol CHARACTER (3) NOT NULL,
    lender_name CHARACTER VARYING (100),
    PRIMARY KEY (lender_symbol)
) ENCODE AUTO;

CREATE TABLE ppp.dim_borrowers (
    borrower_id INTEGER NOT NULL,
    borrower_name CHARACTER VARYING(100) NOT NULL,
    borrower_address CHARACTER VARYING(100) NOT NULL,
    borrower_city CHARACTER VARYING(50) NOT NULL,
    borrower_state CHARACTER(2) NOT NULL,
    borrower_zip CHARACTER VARYING(10),
    borrower_latitude DECIMAL(18, 4),
    borrower_longitude DECIMAL(18, 4),
    borrower_county CHARACTER VARYING(100),
    PRIMARY KEY (borrower_id)
) ENCODE AUTO;

CREATE TABLE ppp.dim_balancesheets (
    balancesheet_id CHARACTER (8) NOT NULL,
    lender_symbol CHARACTER (3) NOT NULL,
    year CHARACTER(4),
    common_stock_equity BIGINT,
    invested_capital BIGINT,
    net_debt BIGINT,
    net_tangible_assets BIGINT,
    ordinary_shares_number BIGINT,
    preferred_shares_number BIGINT,
    preferred_stock_equity BIGINT,
    share_issued BIGINT,
    tangible_book_value BIGINT,
    total_assets BIGINT,
    total_capitalization BIGINT,
    total_debt BIGINT,
    total_equity_gross_minority_interest BIGINT,
    total_liabilities_net_minority_interest BIGINT,
    treasury_shares_number BIGINT,
    PRIMARY KEY (balancesheet_id),
    CONSTRAINT fk_lender
    FOREIGN KEY (lender_symbol)
    REFERENCES ppp.dim_lenders (lender_symbol)
) ENCODE AUTO;

CREATE TABLE ppp.fact_stocks(
    stock_id INTEGER NOT NULL,
    lender_symbol CHARACTER(3),
    stock_date DATE,
    stock_price DECIMAL,
    PRIMARY KEY(stock_id),
    CONSTRAINT fk_lender
    FOREIGN KEY (lender_symbol)
    REFERENCES ppp.dim_lenders (lender_symbol)
) ENCODE AUTO;

CREATE TABLE ppp.fact_loans (
    loan_number BIGINT NOT NULL,
    date_approved date,
    initial_approval_amount DECIMAL(18, 2),
    naics_code INTEGER,
    forgiveness_amount DECIMAL(18, 2),
    forgiveness_date date,
    jobs_reported INTEGER,
    borrower_id INTEGER,
    lender_symbol CHARACTER (3),
    forgiveness_status boolean,
    PRIMARY KEY (loan_number),
    CONSTRAINT fk_naics
    FOREIGN KEY (naics_code)
    REFERENCES ppp.dim_naics (naics_code),
    CONSTRAINT fk_borrower
    FOREIGN KEY (borrower_id)
    REFERENCES ppp.dim_borrowers (borrower_id),
    CONSTRAINT fk_lender
    FOREIGN KEY (lender_symbol)
    REFERENCES ppp.dim_lenders (lender_symbol)
) ENCODE AUTO;
END;

$$