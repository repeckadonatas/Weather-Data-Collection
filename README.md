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

To use the program, run the _`main.py`_ file. Once started, the API data download will begin, followed by data preparation and then data upload to a table on a database.

The program runs automatically on a set schedule (once per hour every day). The schedule is set using cron. Alternatively, a schedule is also set using Windows Task Scheduler.

Currently, the program retrieves data of the 20 largest cities in Europe. If the need arises to get the data for more cities, a name, country and the coordinates of a city must be placed in a text file in `Source/locations/locations.txt` in the same format as other cites (especially the coordinates).

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

* Avoid using raw SQL statements as it makes database prone to SQL injection attacks;
* 