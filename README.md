# Chicago Crimes Etl 
By Camila Cardona Alzate, Angelica Portocarrero Quintero and Xilena Atenea Rojas Salazar.

## Project Overview
In this project, we are going to analyze, manipulate and visualize data about crimes in the city of Chicago, as part of an ETL project. We used Apache Airflow to create the workflow of the read, transformations, loads and streaming of the data, where we finally created some graphics in PowerBI.
  
## Dataset used
The dataset used in this project is the [Chicago Crime Dataset (2001 -- Present)](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present), imported from Kaggle.
It contains 22 columns and over 7 million rows that correspond to crimes committed in the city of Chicago from 2001 to half of 2023.

## Tools used
- Pandas, to manipulate the data.
- Postgres to connect to the database, create the tables, inserting data and modifying it.
- Apache Airflow for creating the workflow of the ETL.
- Docker to run Kafka and create a topic.
- Kafka to stream the data of the fact table.
- PowerBI to visualize the data from the DB and consume the streamed data.

## Project setup

1. Clone the repository

    ```python
    git clone https://github.com/YOANGELICA/chicago_crimes_etl
    ```

2. Install python 3.x

3. Install Postgres 14.x and create a database.

4. Create a virtual environment

    `python -m venv env`

5. Activate said environment

    `env/scripts/activate`

6. Install the dependencies of the project

    ```python
    pip install -r requirements.txt
    ```
    
7. Install Apache Airflow

    ```python
    pip install apache-airflow
    ```
8. Go to the docker folder and run the docker compose:

    ```bash
    docker-compose up -d
    ```

9. Enter to the Kafka bash and create a new topic with:
    
    ```bash
    docker exec -it kafka-test bash
    kafka-topics --bootstrap-server kafka-test:9092 --create --topic topic_name
    ```
    
10. Create database credentials file 'config_db.json' with the following structure and fill it with your own credentials:
    ```
    {
      "user":"",
      "password":"",
      "host":"",
      "server": "",
      "db":""
    }
    ```
    
11. Download the [dataset](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present) from Kaggle and insert it into a '/data' folder.

12. Launch Airflow 

    ```bash
    airflow standalone
    ```
13. Change the file airflow.cfg and add the path to the api_dag folder so it can be added the etl_project dag.
14. After the dag ends, run in another console the consumer.py that is in the folder api_dag to send the streamed data to PowerBI.
15. To connect the streaming data to PowerBI add an API endpoint from this visualization tool, follow this [link](https://desarrollopowerbi.com/dashboard-en-tiempo-real-con-apacha-kafka-python-y-power-bi/) for more information.
