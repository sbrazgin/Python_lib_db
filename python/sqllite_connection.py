# author: Brazgin Sergey / sbrazgin@gmail.com / 06.2021

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API for work with SqlLite DB
"""

import sqlite3
import abc_conn
import logging

NAME_TYPE_1 = 'sqlite'


# =======================================================================
class SqlLiteConn(abc_conn.DbBaseConn):

    # -------------------------------------------------------------------
    def __init__(self, file_name):
        db_desc = f'File name = {file_name}'

        self.__file_name = file_name
        super().__init__(db_desc)

    # -------------------------------------------------------------------
    def connect(self):
        try:
            conn = sqlite3.connect(self.__file_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            logging.info('sqlite3.version = ')
            logging.info(sqlite3.version)
            # print(sqlite3.version)
            super().set_connect(conn)
        except Exception as err:
            super().set_conn_error(str(err))
            logging.error(err)

        return super().get_conn()

    # -------------------------------------------------------------------
    def get_type(self) -> str:
        return NAME_TYPE_1

    # -------------------------------------------------------------------
    def get_table_parts(self, schema: str, name: str) -> int:
        return 0

    # -------------------------------------------------------------------
    def get_sql_fields(self, sql_text: str) -> list:
        return []

    # -------------------------------------------------------------------
    # TODO
    def check_connect(self, p_desc: str = ''):
        if self.is_connected:
            logging.info(f'Connected {p_desc} OK, File name: ' + self.__file_name)
        else:
            err = "Not connected ?"
            logging.error(err)
            super().set_conn_error(str(err))

    # -------------------------------------------------------------------
    # TODO
    def col_to_insert(self, p_name: str, p_cols: list) -> str:
        return ''


# -------------------------------------------------------------------
def check_type(p_type: str) -> bool:
    if p_type == NAME_TYPE_1:
        return True
    return False
