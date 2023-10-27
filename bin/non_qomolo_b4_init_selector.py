#!/usr/bin/env python3
# encoding=utf-8
'''
    从pipeline中筛选在途非Q项目
'''
import numpy as np
import pandas as pd
from db_connector import get_db_connect
from basic_functions import ifnull

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def get_revenue_forecast_from_pipeline(
    database_bo_connector:str,
) -> pd.DataFrame | None:
    '''
    Function:
        Calculate the amount and date of project delivery from table "pipeline_updating"
        which is from database "database_bo".
    Arguments:
        database_bo_connector:
            a line of strings containing username, password, IP, name for pandas to connect mysql database (operation) tables.
    Returns:
        a dataframe.
    '''
    ppl = pd.read_sql("pipeline_updating", database_bo_connector,index_col='id')
    ppl['预计/实际交付时间'] = ppl[['交付预测初验时间', '客户需求交付时间']].apply(
        lambda x: x['交付预测初验时间'] if ((x['交付预测初验时间']==x['交付预测初验时间']) & (x['交付预测初验时间'] != None)) else x['客户需求交付时间'],
        axis=1
    )
    ppl['预计交付金额'] = ppl[['交付预测初验时间', '交付比例', '总金额-元（不含税）']].apply(
        lambda x: ifnull(x['交付比例'],1)*x['总金额-元（不含税）'] if ((x['交付预测初验时间']==x['交付预测初验时间']) & (x['交付预测初验时间'] != None)) else x['总金额-元（不含税）'],
        axis=1
    )
    
    return ppl

def select_non_qomolo_b4_init(
    database_bo_connector: str,
) -> pd.DataFrame | None:
    '''
    Function:
        Select non-qomolo delivery forecast info from pre-processed pipeline.
    Arguments:
        database_bo_connector:
            a line of strings containing username, password, IP, name for pandas to connect mysql database (operation) tables.
    Returns:
        a dataframe.
    '''
    df = get_revenue_forecast_from_pipeline(database_bo_connector)
    df['数据来源'] ='交付'
    df['运营状态'] ='非Q-在途'
    df = df.loc[(
        ((df['签约状态']=='On-going') | (df['签约状态'] == 'Sign')) 
        & ((df['PRJ']!=df['PRJ']) | (df['PRJ']==None)) 
        & (df['产品线2'].isin(['Q-Truck', 'Q-Chassis', 'E-Truck', 'Q-Power', 'AIGT']))
        ),:].copy()
    df['交付信心'] = df['交付信心'].apply(lambda x: None if x=="" else x)
    df['预计/实际交付时间'] = df['预计/实际交付时间'].apply(lambda x: None if x=="" else x)
    df['项目状态'] = "待初验"
    df['确认交付金额'] = None
    df.rename(
        {'总金额-元（不含税）':'合同金额（未税）',
         'REGEXP_Year': 'record_year',
         'REGEXP_Month':'record_month',
         'REGEXP_Week': 'record_week'},
        axis=1,
        inplace=True
    )
    df['是否海外'] = df['国家'].apply(lambda x: 'N' if x=='中国' else 'Y')
    df = df[[
        '数据来源', '运营状态', 'BO', 'PRJ', '项目名称', '产品事业部', 'KA客户', '业务区域', '业务线', '是否海外', 
		'签约信心', '交付信心', '产品线1', '产品线2', '国家','省份','城市','落地港口', '项目状态', '预计/实际交付时间',
        '数量', '确认交付金额', '合同金额（未税）', '预计交付金额', 'updated_at', 'record_year', 'record_month', 'record_week'
    ]].copy()
    ## 筛选掉没有预计交付时间，以及pipeline的月度记录
    df.dropna(subset=['预计/实际交付时间','record_week'],axis=0,inplace=True, how='any')
    return df

if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please.")
    
    database_bo_connector = get_db_connect(
        database_ip=DATABASE_IP,
        database_name=DATABASE_NAME_BO,
        database_username=DATABASE_USERNAME,
        database_password=pswd,
    )
    df = select_non_qomolo_b4_init(database_bo_connector)
    
    print(df)
