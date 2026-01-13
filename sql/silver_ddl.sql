create table if not exists silver.customers (
    customer_id bigint primary key,
    gender varchar(10),
    age int,
    account_open_date date
);

create table if not exists silver.accounts (
    account_id bigint primary key,
    customer_id bigint,
    account_type varchar(20),
    balance numeric(15,2),
    status varchar(20)
);

create table if not exists silver.transactions (
    txn_id bigint primary key,
    account_id bigint,
    txn_date date,
    txn_type varchar(20),
    amount numeric(15,2)
);

create table if not exists silver.loans (
    loan_id bigint primary key,
    customer_id bigint,
    loan_type varchar(30),
    loan_amount numeric(15,2),
    start_date date,
    term_months int,
    interest_rate numeric(5,2),
    status varchar(20)
);

create table if not exists silver.credit_cards (
    cc_id bigint primary key,
    customer_id bigint,
    card_type varchar(20),
    credit_limit numeric(15,2),
    usage_last_month numeric(15,2),
    status varchar(20)
);
