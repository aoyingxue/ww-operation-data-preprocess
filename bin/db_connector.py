#!/usr/bin/env python3
# encoding =utf-8
'''
    Update production schedule with confidence info maintained offline.
'''
from urllib.parse import quote_plus as urlquote

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

######################### Connection #########################
def get_db_connect(database_username,database_password,database_ip,database_name):
    '''
    Function:
        Get access to the database.
    Returns:
        A string used for pandas to connect mysql database tables.
    '''
    db_connect = f'mysql+mysqlconnector://{database_username}:{urlquote(database_password)}@{database_ip}:3306/{database_name}?charset=utf8'
    return db_connect
