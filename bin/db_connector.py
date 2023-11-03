#!/usr/bin/env python3
# encoding =utf-8
'''
    Update production schedule with confidence info maintained offline.
'''
from urllib.parse import quote_plus as urlquote
from sqlalchemy import URL, create_engine, Engine
import pandas as pd

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def get_db_connect(
    database_username: str,
    database_password: str,
    database_ip: str,
    database_name: str,
) -> str:
    '''
    Function:
        Get access to the database.
    Returns:
        A string used for pandas to connect mysql database tables.
    '''
    db_connect = f'mysql+mysqlconnector://{database_username}:{urlquote(database_password)}@{database_ip}:3306/{database_name}?charset=utf8'
    return db_connect

def create_engine_ww(
    database_name: str,
    database_pwd: str,
) -> Engine:
    '''
    Function:
        Create a sqlalchemy engine for connecting westwell operations databases.
    Arguments:
        database_name:
            a string indicating the name of the database.
        database_psw:
            database password.
    Returns:
        An sqlalchemy engine.
    '''
    DATABASE_USERNAME = 'root'
    DATABASE_IP = '192.168.118.100'
    if database_name in ['database_bo', 'operation', 'SalesQuota', 'ScenarioPlanning']:
        url = URL.create(
            "mysql+mysqlconnector",
            username=DATABASE_USERNAME,
            password=database_pwd,  # plain (unescaped) text
            host=DATABASE_IP,
            port="3306",
            database=database_name,
        )
        engine = create_engine(url)
        return engine
    else:
        raise Exception(
            f"{database_name} is not in the westwell operations server.")


if __name__ == '__main__':
    database_pswd = input("Password please: ")
    engine = create_engine_ww('operation', database_pswd)
    project = pd.read_sql("project", engine)
    print(project.head(1))
