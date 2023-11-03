#!/usr/bin/env python3
# encoding=utf-8
'''
    为union运营表做预处理准备
'''
import pandas as pd
from bo_info_merger import merge_bo_info_by_prj_num
from db_connector import create_engine_ww
from pipeline_selector import select_latest_pipeline

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def select_cash_collection(
    df: pd.DataFrame,
    df_project: pd.DataFrame,
    df_pipeline: pd.DataFrame,
    extract_cols = [
        'id', 'ori_index', '数据来源', 'BO', 'PRJ', '项目名称', '运营状态', 
        '产品事业部', 'KA客户', '业务区域', '业务线', '国家', '省份', '城市', '落地港口',
        '产品线1', '产品线2', '日期', '金额', 
        'milestone', 'record_year', 'record_month', 'record_week', 'updated_at',
    ],
) -> pd.DataFrame | None:
    '''
    Functions:
        Select columns from cash_collection table, reformat before joining bo info.
    Arguments:
        cash_collection dataframe. 
    Returns:
        A dataframe after preprocess.
    '''
    try:
        df.reset_index(drop=False, inplace=True)
        df['日期'] = df[['预计回收时间','实际回收时间']].apply(
            lambda x: 
                x['预计回收时间'] if ((x['实际回收时间']==None) | (x['实际回收时间']!=x['实际回收时间'])) 
                else x['实际回收时间'], 
            axis=1,
        )
        df.rename(
            columns={
                'UID':'ori_index',
                '状态':'运营状态',
            },
            inplace=True,
        )
        df_merged = merge_bo_info_by_prj_num(df, df_project, df_pipeline)
        df_merged['id'] = "cc_"+df_merged.index.astype(str)
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

    cash_collection = pd.read_sql(
        'cash_collection', scenario_planning_engine, index_col='UID')
    project_business = pd.read_sql('project_business', operation_engine)
    pipeline = select_latest_pipeline(database_bo_engine)
    output = select_cash_collection(cash_collection, project_business, pipeline)
    print(output)