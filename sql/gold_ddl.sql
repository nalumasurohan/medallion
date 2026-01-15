-- gold.customer_financial_summary

drop table if exists gold.customer_financial_summary;

create table gold.customer_financial_summary as
select
    c.customer_id,
    c.gender,
    c.age,

    count(distinct a.account_id) as total_accounts,
  count(
  distinct case when a.status = 'Active' then a.account_id end
) as active_accounts,


    coalesce(sum(a.balance), 0) as total_balance,

    count(distinct t.txn_id) as total_txns,
    coalesce(sum(t.amount), 0) as total_txn_amount,

    count(distinct l.loan_id) as total_loans,
    coalesce(sum(l.loan_amount), 0) as total_loan_amount,

    count(distinct cc.cc_id) as total_credit_cards,
    coalesce(
        sum(cc.usage_last_month) / nullif(sum(cc.credit_limit), 0),
        0
    ) as credit_card_utilization

from silver.customers c
left join silver.accounts a
    on c.customer_id = a.customer_id
left join silver.transactions t
    on a.account_id = t.account_id
left join silver.loans l
    on c.customer_id = l.customer_id
left join silver.credit_cards cc
    on c.customer_id = cc.customer_id
group by
    c.customer_id, c.gender, c.age;

-- gold.monthly_financial_metrics


drop table if exists gold.monthly_financial_metrics;

create table gold.monthly_financial_metrics as
select
    date_trunc(
        'month',
        to_date(t.txn_date, 'MM/DD/YYYY')
    )::date as month,

    count(distinct t.txn_id) as total_transactions,
    sum(t.amount) as total_transaction_amount,

    count(distinct a.account_id) as active_accounts,
    sum(distinct a.balance) as total_balance

from silver.transactions t
join silver.accounts a
    on t.account_id = a.account_id
where lower(a.status) = 'Active'
group by 1
order by 1;


-- gold.dashboard_customer_overview (BI-ready wide table)

drop table if exists gold.dashboard_customer_overview;

create table gold.dashboard_customer_overview as
select
    customer_id,
    gender,
    age,

    case
        when age < 25 then 'Under 25'
        when age between 25 and 34 then '25–34'
        when age between 35 and 44 then '35–44'
        when age between 45 and 54 then '45–54'
        else '55+'
    end as age_bucket,

    total_accounts,
    active_accounts,
    total_balance,
    total_txns,
    total_txn_amount,
    total_loans,
    total_loan_amount,
    total_credit_cards,
    credit_card_utilization,

    case
        when total_balance > 500000 and total_loans = 0 then 'High Value – Low Risk'
        when total_balance > 500000 and total_loans > 0 then 'High Value – Leveraged'
        when total_balance between 100000 and 500000 then 'Mid Value'
        else 'Low Value'
    end as customer_segment

from gold.customer_financial_summary;
