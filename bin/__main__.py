#!/usr/bin/env python3
# encoding=utf-8
'''
    Generate a physical dataframe for revenue forecast, 
    export to MySQL database
    to achive higher efficiency than using views in MySQL.
'''
from sqlalchemy import create_engine, URL
import pandas as pd
from db_connector import get_db_connect
from merge_revenue import merge_revenue
from revenue_union_preprocessor import melt_revenue_for_union
from basic_functions import to_sql_with_pk
from pipeline_selector import select_pipeline

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def main():
    pipeline = select_pipeline(database_bo_connector)
    revenue = merge_revenue(operation_connector, database_bo_connector)
    to_sql_with_pk(
        df = revenue,
        db_engine=scenario_planning_engine,
        db_table_name='revenue',
        pk_label='id',
    )
    
    ## preprocess data sources
    revenue_for_union = melt_revenue_for_union(revenue)
    
    ## final data output
    df_scenarios = pd.concat(
        objs = [pipeline, revenue_for_union],
        axis=0,
        ignore_index=True,
    )
    df_scenarios.reset_index(drop=True, inplace=True)
    df_scenarios.index.name = 'id'
    to_sql_with_pk(
        df = df_scenarios,
        db_engine=scenario_planning_engine,
        db_table_name='scenarios',
        pk_label='id',
    )
    
if __name__=='__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_NAME_SP = 'ScenarioPlanning'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please: ")
    
    scenario_planning_url = URL.create(
        "mysql+mysqlconnector",
        username=DATABASE_USERNAME,
        password=pswd,  # plain (unescaped) text
        host=DATABASE_IP,
        port="3306",
        database=DATABASE_NAME_SP,
    )
    scenario_planning_engine = create_engine(scenario_planning_url)
    
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