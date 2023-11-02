#!/usr/bin/env python3
# encoding=utf-8
'''
    通用functions
'''
import numpy as np
import pandas as pd
from sqlalchemy.types import VARCHAR
from sqlalchemy import text, Engine

__author__ = "Yuki Ao"
__github__ = "aoyingxue"
__copyright__ = "Copyright 2023"


def ifnull(var, val):
    if (var is None) | (var != var):
        return val
    else:
        return var


def to_sql_with_pk(
    df: pd.DataFrame,
    db_table_name: str,
    pk_label: str | None,
    db_engine: Engine,
) -> None:
    '''
    Function:
        export to mysql, alter table and add primary key.
    Arguments:
        df:
            the dataframe to be exported.
        db_table_name:
            the name of the table in mysql database.
        pk_label:
            the column name of primary key.
        db_engine:
            sqlalchemy engine to connect the mysql database.
    Returns:
        None.
    '''
    try:
        if df.index.name != pk_label:
            print(
                f"df index {df.index.name} is not the label you identified. ")
            if pk_label in df.columns:
                df.set_index(pk_label, inplace=True)
            else:
                raise Exception(f"{pk_label} is not one of df's columns.")
        df.to_sql(
            db_table_name,
            db_engine,
            if_exists='replace',
            index_label=pk_label,
            dtype={pk_label: VARCHAR(df.index.get_level_values(
                pk_label).astype('str').str.len().max())},
        )
        print("Export has finished successfully.")
        with db_engine.connect() as con:
            con.execute(
                text(f"ALTER TABLE `{db_table_name}` ADD PRIMARY KEY (`{pk_label}`);"))
        print("Primary key set.")
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


if __name__ == '__main__':
    print(ifnull(np.nan, 0))
