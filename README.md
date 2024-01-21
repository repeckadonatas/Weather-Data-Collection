# Weather Data System


## About (FIX!)


## Tech Stack Used (FIX!)

* Programing language - **Python**;
* Servers and load balancing - for this project, data is stored locally on the machine;
* Data storage and querying - **PostgreSQL**;
* For interactive data cleaning - **Jupyter Notebook**;
* Data cleaning and normalization - **Pandas**;
* Package and dependency management - **Poetry**


## How To Use The Program (FIX!)

To use the program, run the _`main.py`_ file. You will then be greeted with a welcome message. The program uses some pre-set functions for simple ad hoc operations within the database. It also allows for custom SQL queries to be written (proper usage and specific SQL functionality must comply with SQL and PostgreSQL).

To interact with the program use the built-in commands:
*     To test a connection with a database: test
*     To create a table: create
*     To get the list of tables in a database: check
*     To copy data from a CSV file to a specified table: copy
*     To write a custom SQL statement: sql
*     To get a movie or a tv show recommendation based on a genre: rec
*     To quit the program: exit

To see the commands at any time, pass an empty of wrong keyword and then type _**help**_ in the terminal window.


**Note:** 
- To restart the program, run _`main.py`_ again.

### **Important:**

To connect to the database, the `Source/db_config/config.py` file needs to read these values from `credentials.ini` file:

| Variable | Your value    |
|----------|---------------|
| database | database_name |
| host     | host_name     |
| user     | user_name     |
| password | user_password |
| port     | port          |

Store the `credentials.ini` file in `Source/credentials/` folder to connect to the database with no issues.


## Input Dataset Cleaning (FIX!)

Because no data is perfect, data cleaning and normalization was performed while keeping the context of data.




## Future Improvements 