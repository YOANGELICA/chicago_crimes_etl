# Chicago Crimes Etl 
By Camila Cardona Alzate, Angelica Portocarrero Quintero and Xilena Atenea Rojas Salazar.

## Project Overview
In this project, we are going to analyze, manipulate and visualize data about crimes in the city of Chicago, as the first part of an ETL project.
  
## Dataset used
The dataset used in this project is the [Chicago Crime Dataset (2001 -- Present)](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present), imported from Kaggle.
It contains 22 columns and over 7 million rows that correspond to crimes committed in the city of Chicago from 2001 to half of 2023.

## Tools used
- Pandas, to manipulate the data.
- MySQL connector to connect to the database, create the tables, inserting data and modifying it.
- Matplotlib to generate the graphics for this project.

## Project setup

1. Clone the repository

    ```python
    git clone https://github.com/YOANGELICA/chicago_crimes_etl
    ```

2. Install python 3.x

3. Install Mysql Server 8.0 and create a database.

4. Create a virtual environment

    `python -m venv env`

5. Activate said environment

    `env/scripts/activate`

6. Install the dependencies of the project

    ```python
    pip install -r requirements.txt
    ```

7. Create database credentials file 'dconfig_db.json' with the following structure and fill it with your own credentials:
    ```
    {
      "user":"",
      "password":"",
      "host":"",
      "server": "",
      "db":"",
      "auth_plugin":"mysql_native_password"
    }
    ```
    
    > **Note:** It is immportant to state the mysql_native_password as the auth plugin, if you still get an error message despite this, make sure that in your MySQL Server, your user has the right plugin with this command: `select user, plugin from mysql.user;`
    >

8. Download the [dataset](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present) from Kaggle and insert it into a '/data' folder.

9. Launch Jupyter and choose the kernel associated with the recently created virtual environment.

    ```python
    jupyter notebook
    ```
