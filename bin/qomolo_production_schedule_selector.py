#!/usr/bin/env python3
# encoding=utf-8
'''
    从排产表中筛选在手+在途的Q项目
'''
import numpy as np
import pandas as pd
from db_connector import get_db_connect
import datetime as dt

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def select_qomolo_schedule(
    database_bo_connector:str,
):
    '''
    Function:
        Preprocess table "production_schedule" from database "database_bo".
    Arguments:
        database_bo_connector:
            a line of strings containing username, password, IP, name for pandas to connect mysql database (operation) tables.
        a dataframe.
    '''
    production_schedule = pd.read_sql("production_schedule", database_bo_connector, index_col='index')
    production_schedule['数据来源'] ='交付'
    production_schedule['运营状态'] = production_schedule['是否在手'].apply(
        lambda x: 'Q-在途' if x=='N' else (
            'Q-在手' if x=='Y' else 'Q'
        )
    )
    production_schedule['PRJ'] = production_schedule['PRJ'].apply(lambda x: None if x=='NA' else x)
    production_schedule['交付信心'] = production_schedule['交付信心'].apply(lambda x: None if x=="" else x)
    production_schedule['项目状态'] = '待初验'
    production_schedule['确认交付金额'] = None # 排产表中均为未交付
    production_schedule['合同金额（未税）'] = None # placeholder，未来运营模式上系统后可能会需要更新
    production_schedule['record_month'] = production_schedule['updated_at'].apply(lambda x: (x+dt.timedelta(days=-2)).date().month)
    production_schedule.rename(
        {
            '项目':'项目名称',
            '预计交付时间':'预计/实际交付时间',
            '2023年实际备货数量':'数量',
        },
        axis=1,
        inplace=True,
    )
    production_schedule = production_schedule[[
        '数据来源','运营状态', 'BO', 'PRJ', '项目名称', '产品事业部', 'KA客户', '业务区域', '业务线', '是否海外', '签约信心', '交付信心',
        '产品线1', '产品线2','国家','省份','城市','落地港口','项目状态','预计/实际交付时间','数量','确认交付金额','合同金额（未税）',
        '预计交付金额','updated_at','record_year','record_month', 'record_week',
    ]].copy()
    
    ## 筛选掉预计交付日期为空的
    production_schedule.dropna(
        subset=['预计/实际交付时间'],
        axis=0,
        inplace=True, 
        how='any',
    )
    return production_schedule

if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please.")
    
    database_bo_connector = get_db_connect(
        database_ip=DATABASE_IP,
        database_name=DATABASE_NAME_BO,
        database_username=DATABASE_USERNAME,
        database_password=pswd,
    )
    
    df = select_qomolo_schedule(database_bo_connector)
    
    print(df)