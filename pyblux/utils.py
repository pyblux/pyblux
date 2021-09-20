#!/usr/bin/env python
# coding: utf-8

import os,sys
import pandas as pd
import requests
import json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle, postgresql, mysql, mssql
from pyblux.blux import Blux
from typing import AnyStr, Callable

def get_engine(user:str,password:str,host:str,port:int,database:str,dialect:str,verbose:bool=False,parameter:str=None,raw_engine:bool=True,logger:Callable=print):

    """
    Get Engine for Teradata , Oracle, Aurora/Postgres, Aurora/MySql/MariaDB, SQLite, and  Microsoft SQL Server
    Returns
    -------
    Connection Engine Object
    """
    
    stdout=''
    
    if verbose:
        logger('Attempting to connect to {} Database...'.format(dialect))
    engine = None
    try:
        if dialect == 'teradata':
            import teradatasqlalchemy
            if parameter == None:
                parameter = ''
            Connection_String = 'teradatasql://{user}:{password}@{host}:{port}/{database}{parameter}'.format(host=host, user=user, password=password, port=port, database=database, parameter=parameter)
        elif dialect == 'oracle':
            import cx_Oracle as orcl
            dsn = orcl.makedsn(host, port, service_name=database)
            Connection_String = 'oracle://{user}:{password}@{dsn}'.format(user=user, password=password, dsn=dsn)
        elif dialect == 'postgres':
            import psycopg2
            Connection_String = 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(host=host, user=user, password=password, port=port, database=database)
        elif dialect == 'mssql':
            import pyodbc
            Connection_String ='mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server+Native+Client+10.0'.format(host=host, user=user, password=password, port=port, database=database)
        elif dialect == 'sqlite':
            Connection_String ='sqlite://'
        elif dialect == 'mysql':
            import pymysql
            Connection_String ='mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(host=host, user=user, password=password, port=port, database=database)
        if raw_engine:
            engine=create_engine(Connection_String).raw_connection()
        else:
            engine=create_engine(Connection_String)
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    if verbose and engine != None:
        logger('Connected to {} Database......'.format(dialect))
    return engine


def get_connection(user:str,password:str,host:str,port:int,database:str,dialect:str,verbose:bool=False,parameter:str=None,logger:Callable=print):
    """
    Get a regular connection for Teradata , Oracle, Aurora/Postgres, Aurora/MySql/MariaDB, SQLite, and  Microsoft SQL Server
    Returns
    -------
    Connection Object
    """
    stdout=''
    if verbose:
        logger('Attempting to connect to {} Database...'.format(dialect))
    connection = None
    try:
        if dialect == 'teradata':
            import teradatasql
            if parameter == None:
                parameter = ''
            connection = teradatasql.connect(host, user, password, parameter.replace('?',''))
        elif dialect == 'oracle':
            import cx_Oracle as orcl
            connection = orcl.connect(host, user, password) 
        elif dialect == 'postgres':
            connection = psycopg2.connect( host, user, password, port, database)  
        elif dialect == 'mysql':
            import pymysql
            connection = mysql.connect( host, user, password, port, database)  
        elif dialect == 'mssql':
            import pyodbc
            connection = pyodbc.connect(driver=parameter,uid=user,pwd=password,server=host,database=database,port=port)
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    if verbose and connection != None:
        logger('Connected to {} Database......'.format(driver))
    return connection


def is_exist(table:str='', Blux:Blux=None,verbose:bool=False,logger:Callable=print, stdout:str=''):
    """
    Check if table exist for Teradata , Oracle, Aurora/Postgres, Aurora/MySql/MariaDB, SQLite, and  Microsoft SQL Server
    Returns
    -------
    boolean Object
    """
    stdout=''
    try:
        query = {}
        query['postgres']="""select table_name as name FROM information_schema.tables WHERE  table_schema = '{schema}' AND    table_name   = '{name}'
                            union 
                            select matviewname as name  from pg_matviews WHERE  schemaname = '{schema}' AND    matviewname   = '{name}'
                            union
                            select table_name as name FROM information_schema.views WHERE  table_schema = '{schema}' AND    table_name   = '{name}';
                        """
        query['teradata'] = """select tablename as name from dbc.tables where databasename = '{database}' and tablename = '{name}';"""

        query['mssql'] = """select table_name as name from information_schema.tables where table_schema = '{schema}' and table_name = '{name}'
                            union 
                            select table_name as name from information_schema.views where table_schema = '{schema}' and table_name = '{name}'
                        ;"""

        query['mysql'] = """select table_name as name from information_schema.tables where table_schema = '{schema}' and table_name = '{name}'
                            union 
                            select table_name as name from information_schema.views where table_schema = '{schema}' and table_name = '{name}'
                        ;"""

        query['oracle'] = """select table_name as name from all_all_tables where owner=upper('{database}') and table_name =upper('{name}')"""

        query['sqlite'] = """select tbl_name as name from sqlite_schema  where tbl_name = '{name}'"""
        
        
        if verbose:
            logger('Check if the table or view {} Exists in the database......'.format(table))

        if '.' in table and Blux.dialect in ['postgres','mssql','mysql']:
            schema, database, name = table.split('.')[0], '', table.split('.')[1]
        elif '.' in table and Blux.dialect not in ['postgres','mssql','mysql']:
            schema, database, name = '', table.split('.')[0], table.split('.')[1]
        else:
            schema, database, name = '', '', table
        df = Blux.sql(query=query[Blux.dialect].replace('{schema}',schema).replace('{name}',name).replace('{database}',database), verbose=verbose,logger=logger)
        if verbose:
            if df.empty:
                logger(' Table or view {}  Does not Exists in database {}......result-->{}'.format(table,database,not df.empty))
            else:
                logger(' Table or view {}  Already Exists in database {}......result-->{}'.format(table,database,not df.empty))
        return not df.empty
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    return False

def create_table_text(dataframe:pd.DataFrame=None, table:str=None,verbose:bool=False,logger:Callable=print):
    """
    - create_table_text(dataframe, table)
    - dataframe: source dataframe
    - table: target table to be created in database
    - Returns (string): SQL create table statement
    """
    stdout=''
    if verbose:
        logger("\nAttempting to generate SQL create table statement for {}....\n".format(table))  
    try:
        create_table_columns=',\n'.join(['"'+columnname.lower()+'" varchar(255)' for columnname in dataframe.columns])
        create_text="""
        CREATE TABLE    {}
                        (
                        {}
                        );
                        """.format(table,create_table_columns)
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if verbose:
        logger("\nDone with create table statement for {}....\n".format(table))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    return create_text


def drop_table(table:str=None,Blux:Blux=None,verbose:bool=False,logger:Callable=print):
    stdout=''
    if verbose:
        logger("\nAttempting to drop table {}....\n".format(table))
    try:
        if is_exist(table=table,Blux=Blux,verbose=verbose,logger=logger):
            Blux.sql(query="""DROP TABLE {};""".format(table),verbose=verbose,logger=logger)
        else:
            if verbose:
                logger("\n Table {} doesn't exist....\n".format(table))
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    if verbose:
        logger("\nDone with drop table {}....\n".format(table))


def create_table_from_dataframe(dataframe:pd.DataFrame=None,table:str=None,Blux:Blux=None,verbose:bool=False,logger:Callable=print):
    stdout=''
    try:
        sql_create_table_text=create_table_text(dataframe, table)
        if verbose:
            logger("\nAttempting to create table {}....\n".format(table))
            logger("\nCreate Table Text is: {}\n".format(sql_create_table_text))
        drop_table(table=table,Blux=Blux,verbose=verbose,logger=logger)    
        Blux.sql(query=sql_create_table_text, verbose=verbose, logger=logger)
        Blux.sql(dataframe=dataframe,table=table, verbose=verbose, logger=logger)
    except Exception as e:
        stdout = str(e).split("\n")[0] + "\n"
        if verbose:
            logger("### Exception ### \n {}".format(stdout))
    if len(stdout)>0 and verbose:
        sys.exit("### Exception ### \n {}".format(stdout))
    elif len(stdout)>0:
        sys.exit("### Exception - hint: verbose=True to have error details###")
    if verbose:
        logger("\nDone with create table {}....\n".format(table))


def send_teams_notification ( hookurl: str, title: str='' , text: str='', message: str ='', status: str ='', error_message: str='', activitySubtitle: str='', activityText: str=''):
    payload={}
    # Section Title
    payload["title"] = title
    payload["text"] =  text
    error_message = json.dumps(error_message)
     
    if status == "FAILLED":
        facts_name = "Error Message"
        facts_value = error_message
    else:
        facts_name = "Message"
        facts_value = message
        
    payload["sections"] = [
        {
            "activityTitle": status,
            "activitySubtitle": activitySubtitle,
            "activityText":  activityText,
            "activityImage": "https://teamsnodesample.azurewebsites.net/static/img/image5.png"
        },
        {
            "title": "Output",
            "facts": [
                {
                    "name": facts_name,
                    "value": facts_value
                }
            ]
        }
    ]
    headers = {"Content-Type":"application/json"}
    r = requests.post(hookurl, data=json.dumps(payload), headers=headers)
 


def send_email(server:str, port:int,sender: str, receivers: list, subject: str, body_text: str, attachment: any = None,df: pd.DataFrame = None):
    import smtplib
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    subject = subject

    if df is not None:
        df = (
            df.style
            .set_table_styles([{'selector': 'th', 'props': [('font-size', '9pt'), 
                                                            ('font-family', 'Calibri'),
                                                            ('padding', '8px'),
                                                            ('text-align', 'left'),
                                                            ('vertical-align', 'bottom'),
                                                            ('border-bottom', '1px solid black')]}])        
            .set_properties(**{'border-collapse': 'collapse',
                            'border-bottom': '1px solid #ddd',
                            'font-size': '9pt', 
                            'font-family': 'Calibri',
                            'padding': '8px',
                            'width': '75px'})
            .set_precision(2)
            .hide_index()
            .render()
        )
    
    else:
        df = ''

    body = f'''
        <html>
            <body>
                <p>{body_text} <br>
                {df}
                </p>
            </body>
        </html>
    '''
    rec_list = receivers
    sender = sender
    receivers = ', '.join(rec_list)
    
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = receivers
    
    message.attach(MIMEText(body, 'html'))

    if attachment:
        part = MIMEBase('application', 'octate-stream')
        part.set_payload(open(attachment, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename = {attachment.rpartition("/")[-1]}')
        message.attach(part)

    mailServer = smtplib.SMTP(server, port)
    mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()
    mailServer.sendmail(sender, rec_list, message.as_string())
    mailServer.close()
