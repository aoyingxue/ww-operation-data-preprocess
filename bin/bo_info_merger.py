#!/usr/bin/env python3
# encoding=utf-8
'''
    用编号匹配BO信息
'''
import pandas as pd
from db_connector import create_engine_ww
from pipeline_selector import select_latest_pipeline

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def merge_bo_info_by_bo_num(
    left: pd.DataFrame,
    right: pd.DataFrame,
    extract_bo_cols=None,
    key_col='BO',
) -> pd.DataFrame | None:
    '''
    Function:
        Merge any functional dataframe with columns in the dataframe containing BO information on certain key column, 
        extracting columns needed for operations analysis.
    Arguments:
        left:
            A dataframe such as direct_purchase, or cash_collection, which only has project number or business opportunity number.
        right:
            The dataframe containing BO information.
        extract_bo_cols:
            A list of column names that need to be extracted from df_bo.
        key_col:
            The foreign key column matching two dataframes, default value of PRJ.
    Returns:
        a dataframe if running normally, or None returned.
    '''
    try:
        if (extract_bo_cols == None) | (extract_bo_cols != extract_bo_cols):
            extract_bo_cols = [
                '项目名称', '产品事业部', 'KA客户', '业务区域', '业务线', '国家', '省份', 
                '城市', '落地港口', '产品线1', '产品线2',
            ]
        if (key_col != 'BO'):
            print(f"Please be aware that the key you are trying to merge on is {key_col}.")
            extract_bo_cols += [key_col]
        ori_row_num = left.shape[0]
        print(f"The original dataframe has {ori_row_num} rows.")
        df_output = pd.merge(
            left=left,
            right=right[[key_col]+extract_bo_cols],
            on=key_col,
            how='left',
        )
        merged_row_num = df_output.shape[0]
        print(f"The merged dataframe has {merged_row_num} rows.")
        if ori_row_num == merged_row_num:
            print("No duplicated rows are generated after the merge. You may proceed. ")
        else:
            print("Duplicated rows may be generated after the merge. Be careful. ")
        return df_output
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

def find_bo_num_by_prj(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_prj_col='PRJ',
    right_prj_col='PRJ',
    bo_col='BO',
) -> pd.DataFrame | None:
    '''
    Function:
        Find BO number for each row based on its project number. 
    Arguments:
        left:
            A dataframe such as direct_purchase, or cash_collection, which only has project number.
        right:
            The dataframe containing BO and PRJ number information.
        left_prj_col:
            Foreign key of the left dataframe. The default column name is PRJ. If not, it will be renamed as PRJ.
        right_prj_col:
            Foreign key of the right dataframe. The default column name is PRJ. If not, it will be renamed as PRJ.
        bo_col:
            The column name of business opportunity number, usually being 'BO', if not then will be renamed as 'BO'.
    Returns:
        a dataframe containing a new column BO if running normally, or None returned.
    '''
    try:
        if bo_col != 'BO':
            right.rename(columns={bo_col: 'BO'}, inplace=True)
        if right_prj_col != 'PRJ':
            right.rename(columns={right_prj_col: 'PRJ'}, inplace=True)
        if left_prj_col != 'PRJ':
            left.rename(columns={left_prj_col: 'PRJ'}, inplace=True)
        bo = pd.merge(
            left=left,
            right=right[['BO', 'PRJ']].drop_duplicates(
                keep='first', ignore_index=True),
            on='PRJ',
            how='left',
        )
        return bo
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

def merge_bo_info_by_prj_num(
    df: pd.DataFrame,
    df_project: pd.DataFrame,
    df_pipeline: pd.DataFrame,
)->pd.DataFrame | None:
    '''
    Function:
        Merge BO info by project number.
    Arguments:
        df: the dataframe containing other functional data such as cash, direct procurement.
        df_project: table 'project' in database 'operation'.
        df_pipeline: latest pipeline records.
    Returns:
        A dataframe containing bo information found on project number.
    '''
    df_w_bo = find_bo_num_by_prj(
        df,
        df_project,
        bo_col='bo',
        right_prj_col='project_number',
    )
    print(df_w_bo.columns)
    output = merge_bo_info_by_bo_num(
        left=df_w_bo,
        right=df_pipeline,
    )
    return output

if __name__ == '__main__':
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
    output = merge_bo_info_by_prj_num(cash_collection, project_business, pipeline)
    print(output)