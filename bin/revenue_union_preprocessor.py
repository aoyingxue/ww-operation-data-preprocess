#!/usr/bin/env python3
# encoding=utf-8
'''
    将合并后的revenue，重新处理，为union运营大表做准备
'''
import pandas as pd
from db_connector import get_db_connect
from revenue_generator import generate_revenue
from sqlalchemy import create_engine, URL

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def melt_revenue_for_union(
    df_revenue: pd.DataFrame,
) -> pd.DataFrame | None:
    '''
    Function:
        pivot columns and preprocess data for union.
    Arguments:
        df_revenue:
            original dataframe containing revenue info.
    Returns:
        a melted dataframe.
    '''
    df_revenue['信心'] = df_revenue['交付信心'].copy()
    df_melted = df_revenue.melt(
        id_vars=df_revenue.columns[~df_revenue.columns.isin(
            ['确认交付金额', '预计交付金额'])],
        value_vars=['确认交付金额', '预计交付金额'],
        var_name='交付金额状态',
        value_name='金额',
    )
    df_melted['金额'] = pd.to_numeric(df_melted['金额'])
    df_melted = df_melted.loc[((df_melted['金额'] != 0) & (
        (df_melted['金额'] == df_melted['金额']) & (df_melted['金额'] != None))),].copy()  # 预计金额为0或空的筛掉
    df_melted['运营状态'] = df_melted[['运营状态', '交付金额状态']].apply(
        lambda x: '已确认' if x['交付金额状态'] == '确认交付金额' else (
            '非Q-在手' if ((x['运营状态'] == '已确认') & (x['交付金额状态'] == '预计交付金额')) else x['运营状态']),
        axis=1,
    )
    df_melted['信心'] = df_melted[['信心', '交付金额状态']].apply(
        lambda x: 'Closed' if x['交付金额状态'] == '确认交付金额' else x['信心'],
        axis=1,
    )
    # drop unused columns
    df_melted.rename(
        {'预计/实际交付时间': '日期'},
        axis=1,
        inplace=True,
    )
    df_melted = df_melted.drop(
        ['签约信心', '交付信心', '项目状态', '合同金额（未税）', '交付金额状态'],
        axis=1,
    )
    # reset the index after dropping off columns and rows
    df_melted.reset_index(drop=True, inplace=True)
    return df_melted

if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_NAME_SP = 'ScenarioPlanning'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please.")

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
    revenue = generate_revenue(operation_connector, database_bo_connector)
    melt_revenue_for_union(revenue)
