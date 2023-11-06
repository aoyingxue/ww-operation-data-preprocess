#!/usr/bin/env python3
# encoding=utf-8
'''
    从数据库中抽取回款数据，并匹配BO信息
'''
import pandas as pd
from sqlalchemy import Engine
from bo_info_merger import merge_bo_info_by_prj_num
from db_connector import create_engine_ww
from pipeline_selector import select_latest_pipeline

__author__='Yuki Ao'
__github__='aoyingxue'
__copyright__='Copyright 2023'

def select_direct_purchase(
    db_engine: Engine,
    df_project: pd.DataFrame,
    df_pipeline: pd.DataFrame,
    extract_cols = [
        'id', 'ori_index', '数据来源', 'BO', 'PRJ', '项目名称', '运营状态', 
        '产品事业部', 'KA客户', '业务区域', '业务线', '国家', '省份', '城市', '落地港口',
        '产品线1', '产品线2', '日期', '金额', 
        '直采编号',
        'record_year', 'record_month', 'record_week', 'updated_at',
    ],
)-> pd.DataFrame | None:
    try:
        df = pd.read_sql(
            'direct_purchase', db_engine)
        df.rename(
            columns={
                'UID': 'ori_index',
                '付款时间': '日期',
                '支付金额': '金额',
                '状态': '运营状态',
                'ERP编号': '直采编号',
            },
            inplace=True,
        )
        df_merged = merge_bo_info_by_prj_num(df, df_project, df_pipeline)
        df_merged['id'] = "dp_"+df_merged.index.astype(str)
        df_extracted = df_merged[extract_cols].copy()
        df_extracted.index.name = 'id'
        return df_extracted
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    
if __name__=='__main__':
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_NAME_SP = 'ScenarioPlanning'
    pswd = input("Password please: ")
    scenario_planning_engine = create_engine_ww(DATABASE_NAME_SP, pswd)
    operation_engine = create_engine_ww(DATABASE_NAME_OP, pswd)
    database_bo_engine = create_engine_ww(DATABASE_NAME_BO, pswd)
    project_business = pd.read_sql('project_business', operation_engine)
    pipeline = select_latest_pipeline(database_bo_engine)
    output = select_direct_purchase(scenario_planning_engine, project_business, pipeline)
    print(output)