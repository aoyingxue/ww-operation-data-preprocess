#!/usr/bin/env python3
# encoding=utf-8
'''
    Generate a physical dataframe for revenue forecast, 
    export to MySQL database
    to achive higher efficiency than using views in MySQL.
'''
import pandas as pd
from db_connector import get_db_connect
from revenue_generator import generate_revenue
from revenue_union_preprocessor import melt_revenue_for_union
from basic_functions import to_sql_with_pk
from pipeline_selector import select_pipeline_updating, select_latest_pipeline
from db_connector import create_engine_ww
from cash_collection_selector import select_cash_collection

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def main():
    ## 签约数据
    pipeline_updating = select_pipeline_updating(database_bo_engine)
    pipeline = select_latest_pipeline(database_bo_engine)
    
    ## 交付数据
    revenue = generate_revenue(operation_connector, database_bo_connector)
    to_sql_with_pk(
        df=revenue,
        db_engine=scenario_planning_engine,
        db_table_name='revenue',
        pk_label='id',
    )
    # preprocess data sources
    revenue_for_union = melt_revenue_for_union(revenue)
    
    ## 回款数据
    cash_collection = pd.read_sql(
        'cash_collection', scenario_planning_engine, index_col='UID')
    project_business = pd.read_sql('project_business', operation_connector)
    cash_for_union = select_cash_collection(cash_collection, project_business, pipeline)

    # final data output
    df_scenarios = pd.concat(
        objs=[pipeline_updating, revenue_for_union, cash_for_union],
        axis=0,
        ignore_index=True,
    )
    df_scenarios.reset_index(drop=True, inplace=True)
    df_scenarios = df_scenarios.drop(['是否海外','id'], axis=1)
    to_sql_with_pk(
        df=df_scenarios,
        db_engine=scenario_planning_engine,
        db_table_name='scenarios',
        pk_label='id',
    )

if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_NAME_SP = 'ScenarioPlanning'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please: ")

    database_bo_engine = create_engine_ww(DATABASE_NAME_BO, pswd)
    scenario_planning_engine = create_engine_ww(DATABASE_NAME_SP, pswd)
    operation_connector = get_db_connect(
        database_ip=DATABASE_IP,
        database_name=DATABASE_NAME_OP,
        database_username=DATABASE_USERNAME,
        database_password=pswd,
    )
    database_bo_connector = get_db_connect(
        database_ip=DATABASE_IP,
        database_name=DATABASE_NAME_BO,
        database_username=DATABASE_USERNAME,
        database_password=pswd,
    )

    main()
