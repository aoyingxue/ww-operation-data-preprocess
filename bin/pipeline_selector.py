#!/usr/bin/env python3
# encoding=utf-8
'''
    从pipeline中筛选出有效BO
'''
import pandas as pd
from db_connector import get_db_connect
from sqlalchemy import create_engine, URL

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def select_pipeline_updating(
    connector=None,
    engine=None,
    extracted_cols=[
        'id', 'ori_index','数据来源', 'BO', 'PRJ', '项目名称', '运营状态', '信心', '产品事业部', 'KA客户', '业务区域', '业务线',
        '国家', '省份', '城市', '落地港口', '产品线1', '产品线2', '日期', '数量', '金额', 
        'record_year', 'record_month', 'record_week', 'updated_at'
    ],
) -> pd.DataFrame | None:
    '''
    Function:
        filter info needed and preprocess pipeline weekly snapshots for union.
    Arguments:
        database_bo_connector:
            a line of strings containing username, password, IP, name for pandas to connect mysql database (database_bo) tables.
    Returns:
        a preprocessed dataframe.
    '''
    if (connector!=None) & (connector==connector):
        conn = connector
    elif (engine!=None) & (engine==engine):
        conn = engine
    else:
        raise Exception("You are not providing valid MySQL connector.")
    df = pd.read_sql("pipeline_updating",
                     conn, index_col='id')
    df['数据来源'] = '签约'
    df['运营状态'] = df['签约状态'].apply(
        lambda x:
            '预测' if x == 'On-going'
            else ('已确认' if x == 'Sign' else '已取消')
    )
    df['信心'] = df[['签约信心', '签约状态']].apply(
        lambda x:
            'Closed' if x['签约状态'] == 'Sign' else x['签约信心'],
        axis=1
    )
    df.rename(
        {
            '预计/实际签约时间': '日期',
            '总金额-元（含税）': '金额',
            'REGEXP_Year': 'record_year',
            'REGEXP_Month': 'record_month',
            'REGEXP_Week': 'record_week',
        },
        axis=1,
        inplace=True,
    )
    # 筛选掉已取消的商机&月度记录
    df = df.loc[
        ((df['record_week'] != None) & (df['record_week']
         == df['record_week'])) & (df['运营状态'] != '已取消'),
    ].copy()

    # 重设index
    df.reset_index(names=['ori_index'], inplace=True)
    df['id'] = "ppl_"+df.index.astype(str)
    # 筛选列
    df = df[extracted_cols].copy()
    df = df.set_index('id')
    return df

def select_latest_pipeline(
    connector=None,
    engine=None,
)->pd.DataFrame | None:
    if (connector!=None) & (connector==connector):
        conn = connector
    elif (engine!=None) & (engine==engine):
        conn = engine
    else:
        raise Exception("You are not providing valid MySQL connector.")
    df = pd.read_sql(
        '''
        SELECT a.* 
        FROM 
            pipeline_updating a
        INNER JOIN 
            (SELECT BO, MAX(updated_at) AS max_updated_at FROM pipeline_updating
            GROUP BY BO) p
        WHERE 
            a.BO = p.BO AND a.updated_at = p.max_updated_at
            AND a.BO NOT IN ("target","TBD")''', 
        conn, 
        index_col='id',
    )
    return df
        

if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_NAME_SP = 'ScenarioPlanning'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please: ")

    database_bo_connector = get_db_connect(
        database_ip=DATABASE_IP,
        database_name=DATABASE_NAME_BO,
        database_username=DATABASE_USERNAME,
        database_password=pswd,
    )
    scenario_planning_url = URL.create(
        "mysql+mysqlconnector",
        username=DATABASE_USERNAME,
        password=pswd,  # plain (unescaped) text
        host=DATABASE_IP,
        port="3306",
        database=DATABASE_NAME_SP,
    )
    scenario_planning_engine = create_engine(scenario_planning_url)

    df = select_pipeline_updating(database_bo_connector)
    select_latest_pipeline(database_bo_connector)