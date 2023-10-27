#!/usr/bin/env python3
# encoding=utf-8
'''
    从项目总表商务表中筛选在手非Q项目
'''
import numpy as np
import pandas as pd
from db_connector import get_db_connect
from basic_functions import ifnull
import datetime as dt

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"

def select_non_qomolo_in_hand(
    operation_connector: str, 
    database_bo_connector:str,
) -> pd.DataFrame | None:
    '''
    Function:
        Select data from project_business in database "operation", 
        pre-process and re-format, 
        join latest pipeline and get business info.
    Arguments:
        operation_connector: 
            a line of strings containing username, password, IP, name for pandas to connect mysql database (operation) tables.
        database_bo_connector:
            similar strings as above.
    Returns:
        a dataframe.
    '''
    prj = pd.read_sql("project_business", operation_connector,index_col='id')
    ppl = pd.read_sql("pipeline_updating", database_bo_connector,index_col='id')
    prj['数据来源'] ='交付'
    prj['运营状态'] = prj.apply(
        lambda x:
            '已确认' if float(x['confirm_amount'])>0
            else (
                '非Q-在手' if (x['line2'] not in ['Q-Truck', 'Q-Chassis', 'E-Truck', 'Q-Power', 'AIGT']) # 已确认中不筛选Q产品线，仅非Q部分筛选
                else '其他'
            ),
        axis=1
    )
    prj_non_q = prj.loc[prj['运营状态']!='其他',].copy()
    prj_non_q['预计交付金额'] = prj_non_q[['confirm_amount','contract_amount','line2']].apply(
        lambda x: 
            float(ifnull(x['contract_amount'],0))-float(ifnull(x['confirm_amount'],0)) 
            if (x['line2'] not in ['Q-Truck', 'Q-Chassis', 'E-Truck', 'Q-Power', 'AIGT']) 
            else 0, # 筛选掉Q的预计金额，保留Q的已确认金额
        axis=1,
    )
    
    prj_non_q['record_year'] = prj_non_q['created_at'].apply(lambda x: (x+dt.timedelta(days=-2)).date().year)
    prj_non_q['record_month'] = prj_non_q['created_at'].apply(lambda x: (x+dt.timedelta(days=-2)).date().month)
    prj_non_q['record_week'] = prj_non_q['created_at'].apply(lambda x: (x+dt.timedelta(days=-2)).date().isocalendar().week)
    
    prj_non_q.rename(
        {'bo':'BO',
        'project_number': 'PRJ',
        'project_name': '项目名称',
        'prd_division':'产品事业部',
        'ka_customer': 'KA客户',
        'area':'业务区域',
        'division':'业务线',
        'line1':'产品线1',
        'line2':'产品线2',
        'acceptance_status':'项目状态',
        'initial_acceptance_at':'预计/实际交付时间',
        'count':'数量',
        'delivered_confidence':'交付信心',
        'confirm_amount':'确认交付金额',
        'contract_amount':'合同金额（未税）',
        'created_at': 'updated_at',
        },
        axis=1,
        inplace=True,
    )
    
    ppl['record_num'] = ppl.apply(
        lambda x: 
            generate_record_num(
                x['REGEXP_Year'], 
                x['REGEXP_Month'],
                x['REGEXP_Week'],
            ),
        axis=1,
    )
    ppl_latest = ppl.loc[ppl['record_num']==ppl['record_num'].max(),] # 最新一周的pipeline记录
    if_bo_duplicated = ppl_latest.duplicated(subset=['BO']).any()
    print("Is there any duplicated BO in the latest pipeline?", if_bo_duplicated)
    if if_bo_duplicated:
        ppl_latest.drop_duplicates(subset=['BO'], keep='first', inplace=True)
        
    ## merge project_business with the latest pipeline
    df_merge = pd.merge(
        left = prj_non_q,
        right = ppl_latest[['BO', '国家', '省份', '城市', '落地港口', '签约信心']],
        on = 'BO',
        how='left',
    )    
    df_merge['是否海外'] = df_merge['国家'].apply(lambda x: 'N' if x=='中国' else 'Y')
    
    df_merge = df_merge[[
        '数据来源', '运营状态', 'BO', 'PRJ', '项目名称', '产品事业部', 'KA客户', '业务区域', '业务线', '是否海外',
        '签约信心', '交付信心', '产品线1', '产品线2', '国家', '省份', '城市', '落地港口', 
        '项目状态', '预计/实际交付时间', '数量', '确认交付金额', '预计交付金额', 
        'updated_at', 'record_year', 'record_month', 'record_week',
    ]].copy()
    
    ## 筛选掉预计交付日期为空的
    df_merge.dropna(
        subset=['预计/实际交付时间'],
        axis=0,
        inplace=True, 
        how='any',
    )
    return df_merge
    
def generate_record_num(
    yearnum: str, 
    monthnum: str, 
    weeknum: str | None,
) -> str | None:
    yearnum = str(int(yearnum))
    monthnum = "0"+str(int(monthnum)) if len(str(int(monthnum)))<2 else str(int(monthnum))
    if (weeknum!=None) & (weeknum==weeknum):
        weeknum = "0"+str(weeknum) if len(str(int(np.floor(weeknum))))<2 else str(weeknum) ## in case smt like week 9.5
        return float(yearnum+monthnum+weeknum)
    else:
        return None
    
if __name__ == '__main__':
    DATABASE_USERNAME = 'root'
    DATABASE_NAME_BO = 'database_bo'
    DATABASE_NAME_OP = 'operation'
    DATABASE_IP = '192.168.118.100'
    pswd = input("Password please.")
    
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
    select_non_qomolo_in_hand(operation_connector, database_bo_connector)