# DataWarehouseDataLake
- Authors:	Benjamin Hohl and Malik Sogukoglu 
- Group:		12
- Lecturers:	José Mancera, Michael Kaufmann and Luis Terán
- Module:		Data Warehouse and Data Lake Systems
- Date:		April 21, 2021

In this project, a data pipeline was developed in Apache Airflow in combination with the PostgreSQL database. The intention was to explore the real estate market in Seattle and Boston with the given data in order to establish a suitable investment plan in these areas.

## Prequisites
- around 23 GB of free space
- 4GB Ram
- Linux or Linux capable shell-environmnet
- Webbrowser
- optional: PowerBI being pre-installed
- optional: R being pre-installed

## Folder directory
- ```/airbnb anaylsis r.R``` - Pre-analysis of Airbnb data
- ```/local_attempt_2/dags/airbnb.py``` - ETL data pipeline including data warehouse and data marts, implemented in Apache Airflow. Processes Airbnb data and weather data.
- ```/airbnb visualization.pbix``` - Visualisations of the data in PowerBI
