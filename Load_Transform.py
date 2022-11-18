from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


dag= DAG(dag_id= 'Load_Transform', start_date = datetime.today(), catchup=False, schedule_interval='@once')

query1 = '''
         SELECT InvoiceNo, StockCode, Description, Quantity, PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate) AS InvoiceDate, UnitPrice, CustomerID,Country 
            FROM EXTERNAL_QUERY
            (
             "projects/potent-hue-360410/locations/us/connections/load_sql_data",
             "SELECT * FROM customer.customer_details"
            );
         '''

Load_Data = BigQueryOperator( task_id='Load_Data', 
                            destination_dataset_table= "potent-hue-360410.project.customer_data",
                            sql=query1,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

query2 = '''
            delete from `potent-hue-360410.project.customer_data`
            where Quantity  < 0 or CustomerID = 0;
         '''

filter_task1 = BigQueryOperator( task_id='filter_task1', 
                            sql=query2,
                            use_legacy_sql=False,
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

query3 = '''
            delete from `potent-hue-360410.project.customer_data`
            where  EXTRACT(year FROM InvoiceDate)  = 2011 and EXTRACT(month FROM InvoiceDate) = 12;
         '''

filter_task2 = BigQueryOperator( task_id='filter_task2', 
                            sql=query3,
                            use_legacy_sql=False,
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

query4 = '''
            SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID,Country,Quantity * UnitPrice as ItemTotal
            FROM `potent-hue-360410.project.customer_data`;
         '''

ONLINE_RETAIL = BigQueryOperator( task_id='ONLINE_RETAIL', 
                            destination_dataset_table= "potent-hue-360410.project.ONLINE_RETAIL",
                            sql=query4,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

query5 = '''
            select CustomerID, sum(ItemTotal) as TotalSales,count(Quantity) as OrderCount, avg(ItemTotal) as AvgOrderValue 
            from `potent-hue-360410.project.ONLINE_RETAIL`
            group by CustomerID;
         '''

CUSTOMARY_SUMMARY = BigQueryOperator( task_id='CUSTOMARY_SUMMARY', 
                            destination_dataset_table= "potent-hue-360410.project.CUSTOMARY_SUMMARY",
                            sql=query5,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

query6 = '''
            select Country, sum(ItemTotal) as TotalSales, sum(ItemTotal)*100/(select sum(ItemTotal) 
            from `potent-hue-360410.project.ONLINE_RETAIL`) as PercentofCountrySales from `potent-hue-360410.project.ONLINE_RETAIL` 
            group by Country;
         '''

SALES_SUMMARY = BigQueryOperator( task_id='SALES_SUMMARY', 
                            destination_dataset_table= "potent-hue-360410.project.SALES_SUMMARY",
                            sql=query6,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)

Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> Load_Data >> filter_task1 >> filter_task2 >> ONLINE_RETAIL >> [CUSTOMARY_SUMMARY, SALES_SUMMARY] >> End