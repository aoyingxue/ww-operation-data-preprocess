#!/usr/bin/env python3
# encoding=utf-8
'''
    合并三个交付数据源，并导出数据至数据库。
'''
import pandas as pd
from non_qomolo_in_hand_selector import select_non_qomolo_in_hand
from non_qomolo_b4_init_selector import select_non_qomolo_b4_init
from qomolo_production_schedule_selector import select_qomolo_schedule

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def generate_revenue(
    operation_connector: str, 
    database_bo_connector:str, 
) -> pd.DataFrame | None:
    '''
    Function:
        Export merged revenue forecast table to MySQL and return a dataframe.
    Arguments:
        database_bo_connector:
            a line of strings containing username, password, IP, name for pandas to connect mysql database (operation) tables.
    Returns:
        a dataframe.
    '''
    non_qomolo_in_hand = select_non_qomolo_in_hand(operation_connector, database_bo_connector) # 在手非Q
    non_qomolo_b4_init = select_non_qomolo_b4_init(database_bo_connector) # 在途非Q
    qomolo_schedule = select_qomolo_schedule(database_bo_connector) # 在手/在途Q
    df_merge = pd.concat(
        [non_qomolo_in_hand, non_qomolo_b4_init, qomolo_schedule],
        axis=0,
        ignore_index=False,
    )
    df_merge.reset_index(names=['ori_index'], inplace=True)
    df_merge['id'] = "mrf_"+df_merge.index.astype(str) # merged revenue forecast
    # df_merge = df_merge.set_index('id')
    return df_merge