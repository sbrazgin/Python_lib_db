# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020

import logging
import abc_conn

NAME_TYPE_1 = 'csv'


# =======================================================================
class CsvConn(abc_conn.DbBaseConn03):

    def __init__(self, p_one_db: dict, info_msg: str = ''):
        super().__init__('csv', info_msg)

        self.__host_out_dir = None
        if abc_conn.DB_DIR in p_one_db:
            self.__host_out_dir = p_one_db[abc_conn.DB_DIR]

    # -------------------------------------------------------------------
    @property
    def out_dir(self) -> str:
        return self.__host_out_dir

    # -------------------------------------------------------------------
    @out_dir.setter
    def out_dir(self, v):
        self.__host_out_dir = v

    # -------------------------------------------------------------------
    @property
    def is_connected(self) -> bool:
        if self.__host_out_dir:
            return True
        return False

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
    def check_connect(self, p_desc: str = ''):
        if self.__host_out_dir:
            logging.info(f'Connected {p_desc} OK, Dir name: ' + self.__host_out_dir)
        else:
            err = "Dir name is empty!"
            logging.error(err)
            super().set_conn_error(str(err))

    # -------------------------------------------------------------------
    def col_to_insert(self, p_name: str, p_cols: list) -> str:
        return ''


# =========================================================================
# static proc for create connect
# p_one_db - list of params
def create_connect_db(p_one_db: dict, p_config_dir: str = None) -> CsvConn or None:
    if not p_one_db:
        logging.error("Database params for connect empty!")
        return None

    conn = CsvConn(p_one_db)
    # if p_config_dir:
    #    conn.out_dir = f"{p_config_dir}/{conn.out_dir}"

    return conn


# -------------------------------------------------------------------
def check_type(p_type: str) -> bool:
    if p_type == NAME_TYPE_1:
        return True
    return False
