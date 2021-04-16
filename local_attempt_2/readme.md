## Technical documentation local_attempt_2

This part of the code covers the pipeline which includes the data warehouse, the data marts, the processing of the output from the data lake within a workflow. 

The project was implemented in Apache Airflow using PostgreSQL as the database.

### Data processed
- Airbnb data from Boston and Seattle (including housings, transaction log on rentals and reviews of the housings)
- Weather data from Boston and Seattle

### Set up Apache Airflow and PostgreSQL in Docker
- Use Linux or a Linux capable command shell
- Download this project folder and navigate to this "local_attempt_2" folder
- Write ``` docker-compose up airflow-init ``` and 
wait until the whole airflow environment is downloaded and started.
- ...this is only necessary for the first start. For later starts, you can start the service with ```docker-commpose up```
- Go to ```localhost:8080``` in your webbrowser to see the pipeline. In there you will find the pipeline named "taskflow_airbnb2". Click on it to see the individual steps of the pipeline.
- with ```docker-compose down``` you can close Airflow

### Project files
- ```/dags/airbnb.py``` - ETL data pipeline including data warehouse and data marts, implemented in Apache Airflow. Processes Airbnb data and weather data.
- ...you can develop the pipeline further by simply opening the project folder "local_attempt_2" in Visual Studio Code or PyCharm. Go there on ```/dags/airbnb.py``` to edit the data pipeline.
