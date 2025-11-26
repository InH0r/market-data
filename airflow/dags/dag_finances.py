from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


args = {
    'owner': 'person_1',
    'start_date': datetime(2025, 11, 23),
    'provide_context': True
}

with DAG(
    dag_id='finance_etl',
    default_args=args,
    schedule='@hourly',
    tags=['finance'],
    catchup=False
) as dag:

    get = BashOperator(
        task_id='get',
        bash_command='python3 /opt/airflow/scripts/finance/get_data.py',
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/scripts/finance/my-key.json"},
    )

    change = BashOperator(
        task_id='change',
        bash_command='python3 /opt/airflow/scripts/finance/change_data.py',
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/scripts/finance/my-key.json"},
    )

    for_bigquery = BashOperator(
        task_id='for_bigquery',
        bash_command='python3 /opt/airflow/scripts/finance/for_bigquery.py',
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/scripts/finance/my-key.json"},
    )

    operation_market = BigQueryInsertJobOperator(
        task_id='operation_market',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.market` AS
SELECT
  symbol
  ,marketState
  ,sourceInterval
  ,exchangeDataDelayedBy
  ,exchangeTimezoneName
  ,exchangeTimezoneShortName
  ,gmtOffSetMilliseconds
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_company = BigQueryInsertJobOperator(
        task_id='operation_company',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.company_info` AS
SELECT
  symbol
  ,displayName
  ,shortName
  ,longName
  ,market
  ,exchange
  ,quoteType
  ,typeDisp
  ,region
  ,currency
  ,firstTradeDateMilliseconds
  ,messageBoardId
  ,tradeable
  ,cryptoTradeable
  ,esgPopulated
  ,customPriceAlertConfidence
  ,prevName
  ,nameChangeDate
  ,financialCurrency
  ,fullExchangeName
  ,language
  ,quoteSourceName
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_price = BigQueryInsertJobOperator(
        task_id='operation_price',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.price` AS
SELECT
  symbol
  ,regularMarketPrice
  ,regularMarketChange
  ,regularMarketChangePercent
  ,regularMarketOpen
  ,regularMarketDayHigh
  ,regularMarketDayLow
  ,regularMarketVolume
  ,regularMarketPreviousClose
  ,preMarketPrice
  ,preMarketChange
  ,preMarketChangePercent
  ,preMarketTime
  ,ask
  ,bid
  ,askSize
  ,bidSize
  ,marketCap
  ,averageDailyVolume3Month
  ,averageDailyVolume10Day
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_fiftytwoweek = BigQueryInsertJobOperator(
        task_id='operation_fiftytwoweek',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.fiftyTwoWeek` AS
SELECT
  symbol
  ,fiftyTwoWeekHigh
  ,fiftyTwoWeekLow
  ,fiftyTwoWeekChangePercent
  ,fiftyTwoWeekHighChange
  ,fiftyTwoWeekHighChangePercent
  ,fiftyTwoWeekLowChange
  ,fiftyTwoWeekLowChangePercent
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_avg = BigQueryInsertJobOperator(
        task_id='operation_avg',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.averag` AS
SELECT
  symbol
  ,fiftyDayAverage
  ,fiftyDayAverageChange
  ,fiftyDayAverageChangePercent
  ,twoHundredDayAverage
  ,twoHundredDayAverageChange
  ,twoHundredDayAverageChangePercent
  ,averageAnalystRating
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_earnings = BigQueryInsertJobOperator(
        task_id='operation_earnings',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.earnings` AS
SELECT
  symbol
  ,epsTrailingTwelveMonths
  ,epsForward
  ,epsCurrentYear
  ,priceEpsCurrentYear
  ,trailingPE
  ,forwardPE
  ,dividendRate
  ,trailingAnnualDividendRate
  ,dividendYield
  ,trailingAnnualDividendYield
  ,dividendDate
  ,earningsTimestamp
  ,earningsTimestampStart
  ,earningsTimestampEnd
  ,earningsCallTimestampStart
  ,earningsCallTimestampEnd
  ,isEarningsDateEstimate
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_shares = BigQueryInsertJobOperator(
        task_id='operation_shares',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.shares` AS
SELECT
  symbol
  ,priceToBook
  ,bookValue
  ,lastCloseTevEbitLtm
  ,lastClosePriceToNNWCPerShare
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.data_finances_raw`;""",
                "useLegacySql": False
            }
        }
    )

    operation_data_mart = BigQueryInsertJobOperator(
        task_id='operation_data_mart',
        gcp_conn_id='google_connection',
        configuration={
            "query": {
                "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.data_mart_finances` AS
SELECT
  co.symbol
  ,co.shortName AS name
  ,p.regularMarketPrice AS price
  ,p.regularMarketChange
  ,p.preMarketPrice
  ,p.preMarketChange
  ,f.fiftyTwoWeekLow
  ,f.fiftyTwoWeekHigh
  ,a.averageAnalystRating AS rating
  ,e.epsTrailingTwelveMonths
  ,e.dividendYield
  ,p.marketCap
  ,p.regularMarketVolume AS volume
  ,co.fullExchangeName as exchange
  ,CURRENT_DATE() AS date
FROM `global-calling-478611-m1.finances.company_info` co
LEFT JOIN `global-calling-478611-m1.finances.price` p ON co.symbol = p.symbol
LEFT JOIN `global-calling-478611-m1.finances.fiftyTwoWeek` f ON co.symbol = f.symbol
LEFT JOIN `global-calling-478611-m1.finances.averag` a ON co.symbol = a.symbol
LEFT JOIN `global-calling-478611-m1.finances.earnings` e ON co.symbol = e.symbol
;""",
                "useLegacySql": False
            }
        }
    )

    operation_data_mart_top10 = BigQueryInsertJobOperator(
            task_id='operation_data_mart_top10',
            gcp_conn_id='google_connection',
            configuration={
                "query": {
                    "query": """CREATE OR REPLACE TABLE `global-calling-478611-m1.finances.data_mart_finances_top10` AS
SELECT 
    co.symbol
    ,p.regularMarketPrice AS price
    ,p.marketCap 
    ,p.regularMarketChange
    ,f.fiftyTwoWeekHigh
    ,f.fiftyTwoWeekLow
FROM `global-calling-478611-m1.finances.company_info` co
LEFT JOIN `global-calling-478611-m1.finances.price` p ON co.symbol = p.symbol
LEFT JOIN `global-calling-478611-m1.finances.fiftyTwoWeek` f ON co.symbol = f.symbol
;"""
                    ,
                    "useLegacySql": False
                }
            }
        )
    
    get >> change >> for_bigquery >> operation_market >> operation_company >> operation_price >>  operation_fiftytwoweek >> operation_avg >> operation_earnings  >> operation_shares >> operation_data_mart >> operation_data_mart_top10
    