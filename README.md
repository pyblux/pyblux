<div align="center">
  <img src="https://pyblux-images.s3.amazonaws.com/logo.png"><br>
</div>

# pyblux: A suite of fast, easy-to-use, and intuitive Python ETL utilities.

![PyPI Latest Release](https://img.shields.io/pypi/v/pyblux.svg)
[![Package Status](https://img.shields.io/pypi/status/pyblux.svg)](https://pypi.org/project/pyblux/)


## What is it?
> **pyblux** is a Python package that provides a suite of ETL utilities built to make the interactions with databases in the cloud as well as on-premise fast, easy and intuitive.

### Features
+ Support multiple databases, including Postgres, MySql, MS SQL, SQLIte, Teradata and Oracle.
+ The `get_engine` method makes it easy to connect to databases in a simple and intuitive manner.
+ `Blux.sql` method from the `Blux` class helps run fast queries. It Povides output results as namedtuple or dictionary and it supports parameterised queries and in-flight transformation of data. 
+ `Logger` class helps setup logging via log file or console.
+ `send_teams_notification` method provides an easy way to send alerts to a MS Teams channel via an [incoming webhook](https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook).
+ `send_email` method helps to send email with HTML content
+ Helpful error messages display the failed query SQL
+ [DBAPI2 specification](https://www.python.org/dev/peps/pep-0249/) was used in order to simplify coding for queries on relational database systems using Python.

## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/bertin.nono/pyblux

Binary installers for the latest released version are available at the [Python
Package Index (PyPI)](https://pypi.org/project/pyblux)

```sh
# or PyPI
pip install pyblux
``` 

## pyblux provides support for the databases below:

+ [Teradata](###get_engine)
+ [PostgreSQL](###get_engine)
+ [MySQL and MariaDB](###get_engine)
+ [SQLite](###get_engine)
+ [Oracle](###get_engine)
+ [Microsoft SQL Server](###get_engine)

## Dependencies

Depending on the use case, the database package should be installed.

+ [Teradata](https://pypi.org/project/teradatasqlalchemy/) :  Install the Teradata SQL Driver Dialect for SQLAlchemy
```
pip install teradatasqlalchemy
```
+ [PostgreSQL](https://pypi.org/project/psycopg2/): Install Psycopg which is the most popular PostgreSQL database adapter for the Python programming language.
```
pip install psycopg2-binary
```
+ [MySQL and MariaDB](https://pypi.org/project/PyMySQL/): Install the Pure Python MySQL Driver
```
pip install PyMySQL
```
+ [SQLite]: No install required
+ [Oracle](https://pypi.org/project/cx-Oracle/): Intall cx_Oracle which is a Python extension module that enables access to Oracle Database.

```
pip install cx-Oracle
```
+ [Microsoft SQL Server](https://pypi.org/project/pyodbc/): Install the pyodbc which is an open source Python module that makes accessing ODBC databases simple.
```
pip install pyodbc
```

### Documentation

 + [Classes](#classes)
 + [Methods](#methods)
 + [References](#references)

# Classes 

+ ### **Blux:** 
Establishes a connection engine to a database system referenced by the dialect attribute run fast queries.

**Note:** The `dialect` is the system SQLAlchemy uses to communicate with various types of [DBAPI](https://docs.sqlalchemy.org/en/14/dialects/index.html#term-Fully-tested-in-CI) implementations and databases. 

```buildoutcfg
class Blux:
    """
    This class connects to a local database session using the db `dialect` library.
    """

    def __init__(self, engine=None,dialect=None):
        """
        Args:
            engine (str): Database connection engine.
            dialect (str): database system name(postgres, oracle, teradata,...)
        Note: Database Connection package must be installed in order to use this backend.
        """
        self.engine = engine
        self._dialect = dialect

        self.__errlimit = 1
        self.__warnings = []
        self.__errors = []
        self.__logons = []
```
**Example:**

```python
table_list = """SELECT table_schema, table_name FROM information_schema.tables"""
postgres_engine = get_engine(dialect='PG', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")
blux= Blux(engine=postgres_engine, dialect='postgres')
dataframe = blux.sql(query=table_list)
table = 'test'
database = 'postgres'
#load dataframe to table
blux.sql(dataframe=dataframe,database=database,table=table, dialect='postgres')
```


+ ### **Logger:** 
provides a custom logging handler called `logger`. Helps Debug SQL and monitor progress with logging.
```python
class Logger:
    """
    This class connects to a local database session using the db `connnection` library.
    """

    def __init__(self, logname:str, filename:str, level=logging.INFO, console:bool=True):
        """
        Args:
            logname (str): Logger Name.
            filename (str): log file path 
            level (str): Logger Level (DEBUG, INFO, WARNING, ERROR)
            console (cool): print to console
        """
        self._logname = logname
        self._filename = filename
        self._level = level
        self._console = console
```

**Example:**
```python
import logging
from pyblux import logger

pyblux_logger = Logger(logname=ETL.NAME, filename=log_file,level=logging.INFO, console=True)
logger=pyblux_logger.logger( verbose=True)
```

Output from a call for `get_engine` will look like:
```
2021-07-07 15:06:22,411 get_engine: 
2021-07-07 15:06:22,413 get_engine: 
2021-07-07 15:06:22,416 get_engine: 
```

# Methods:

+ ### **get_engine:** 
Creates a database connection engine.

```buildoutcfg
get_engine(user:str,password:str,host:str,port:int,database:str,dialect:str,verbose:bool=False,parameter:str=None,raw_engine:bool=True,logger:Callable=print)
```
Database connection details are defined by `get_engine` objects (see below).

**Example:**
```python
import pandas
from pyblux.utils import get_engine
from pyblux.blux import Blux

oracle_engine = get_engine(dialect='oracle', host="localhost", port=1521,database="mydata", user="oracle_user", password="123")

teradata_engine = get_engine(dialect='mssql', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_engine = get_engine(dialect='mysql', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_engine = get_engine(dialect='teradata', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_engine = get_engine(dialect='postgres', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

```
### Passwords

It is best practice for Database passwords to be stored in environment variables.
This can be done on the command line via:

+ `export password=secret-password` on Linux
+ `set password=secret-password` on Windows

Or in a Python terminal via:

```python
import os
os.environ['password'] = 'secret-password'
```
No password is required for SQLite databases.

+ `When URL that includes the password contains` [Password conatains special characters](https://docs.sqlalchemy.org/en/14/core/engines.html)

**Example:** 
Connection_String = postgresql+psycopg2://user:p@ssword%to%encode@hosturl/defaultdb
The above password encoded using urllib.parse:
```import urllib.parse
   pwd=urllib.parse.quote_plus("p@ssword%to%encode")
   print(pwd)
   Connection_String = postgresql+psycopg2://user:urllib.parse.quote_plus("p@ssword%to%encode")@hosturl/defaultdb
```

+ ### **get_connection:** 
Gets a regular database connection.
```buildoutcfg
get_connection(user:str,password:str,host:str,port:int,database:str,dialect:str,verbose:bool=False,parameter:str=None,logger:Callable=print):
    """
    Get a regular connection for Teradata , Oracle, Aurora/Postgres, Aurora/MySql/MariaDB, SQLite, and  Microsoft SQL Server
    Returns
    -------
    Connection Object
    """
```
**Example:**
```python
from pyblux.utils import get_connection

oracle_conn = get_connection(dialect='oracle', host="localhost", port=1521,database="mydata", user="oracle_user", password="123")

teradata_conn = get_connection(dialect='mssql', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_conn = get_connection(dialect='mysql', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_conn = get_connection(dialect='teradata', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

postgres_conn = get_connection(dialect='postgres', host="localhost", port=5432, database="mydata", user="postgres_user", password="123")

```

+ ### **create_table_from_dataframe:** 
Creates table from a dataframe attributes and fastload data into in it.

```buildoutcfg
create_table_from_dataframe(dataframe:pd.DataFrame=None,table:str=None,Blux:Blux=None,verbose:bool=False,logger:Callable=print)
```

+ ### **is_exist:** 
Checks is a table or view exist.

```buildoutcfg
is_exist(table:str='', Blux:Blux=None,verbose:bool=False,logger:Callable=print)
```
+ ### **drop_table:** 
Checks is a table or view exist and then drops it if it exists.

```buildoutcfg
drop_table(table:str=None,Blux:Blux=None,verbose:bool=False,logger:Callable=print)
```


+ ### **send_teams_notifications:** 
Send a Card to a MS Teams Channel

```buildoutcfg
send_teams_notification ( hookurl: str, title: str='' , text: str='', message: str ='', status: str ='', error_message: str='', activitySubtitle: str='', activityText: str='')
```

+ ### **send_email:** 
Send an HTML formated email that can include a dataframe

```buildoutcfg
send_email(server:str, port:int,sender: str, receivers: list, subject: str, body_text: str, attachment: any = None,df: pd.DataFrame = None)
```

### Maintainers:

+ Bertin Nono  


### Development status

Stable 

### Licence

MIT


## References

+ [psycopg2](http://initd.org/psycopg/docs/cursor.html)
+ [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/cursor.html)
+ [sqlalchemy](https://docs.sqlalchemy.org/en/14/core/engines.html)
 