import airflow
import dbt


# for now, the successful import of airflow and dbt
# shows that the Python interpreter of
# PyCharm could correctly be configured
# to work with our docker compose file

# next steps:

# configure airflow to connect
# to our airflow service run
# by our docker compose file,
# see https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#using-docker-env

# implement the Taskflow API example
# as a basis for our ETL tasks,
# see https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html

# try to code and understand ETL tasks in dbt,
# see https://www.startdataengineering.com/post/dbt-data-build-tool-tutorial/

# then, decide if we want to use
# airflow + pandas
# or airflow + dbt
