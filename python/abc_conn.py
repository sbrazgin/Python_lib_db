# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020

import logging
import abc

# config params for DB
DB_NAME = "name"
DB_TYPE = 'type'
DB_HOST = "host"
DB_PORT = "port"
DB_SERVICE = "service"
DB_DB = "db"
DB_USER = "user"
DB_PASS_FILE = "pass_file"
DB_PASS = "pass"
DB_DIR = "dir"

DB_JSON_NAME = "db"
DB_JSON_LIST = "list_of_databases"


# =======================================================================
class DbBaseConn00(abc.ABC):
    next_id = 0

    # -------------------------------------------------------------------
    def __init__(self, desc_db: str = None, info_msg: str = None):
        self.__desc = desc_db if desc_db else ''
        self.__info = info_msg if info_msg else self.__desc
        self.__conn = None
        self.__param1 = None
        self.__id = DbBaseConn.next_id
        DbBaseConn.next_id += 1

    @property
    def desc(self):
        return self.__desc

    @property
    def info(self):
        return self.__info

    @property
    def conn(self):
        return self.__conn

    @conn.setter
    def conn(self, v):
        self.__conn = v

    @property
    def id(self) -> int:
        return self.__id

    @property
    def is_connected(self) -> bool:
        return True

    def disconnect(self):
        self.conn = None

    def close(self):
        self.disconnect()

    # -------------------------------------------------------------------
    def set_param(self, s):
        self.__param1 = None
        if s:
            self.__param1 = s

    # -------------------------------------------------------------------
    def get_param(self):
        return self.__param1


# =======================================================================
# add ERROR MSG
class DbBaseConn01(DbBaseConn00, abc.ABC):

    # -------------------------------------------------------------------
    def __init__(self, desc_db: str = None, info_msg: str = None):
        super().__init__(desc_db, info_msg)

        self.__conn_error = None
    # -------------------------------------------------------------------

    def get_error(self) -> str:
        return self.__conn_error

    def set_conn_error(self, v):
        if v:
            self.__conn_error = v
            self.disconnect()

    def is_error(self) -> bool:
        if self.__conn_error:
            return True
        else:
            return False


# =======================================================================
# add PARAMS
class DbBaseConn02(DbBaseConn01, abc.ABC):

    # -------------------------------------------------------------------
    def __init__(self, desc_db: str = None, info_msg: str = None):
        super().__init__(desc_db, info_msg)

        self.__param1 = None
        self.__param2 = None

    # -------------------------------------------------------------------
    @property
    def param1(self) -> str:
        return self.__param1

    @param1.setter
    def param1(self, v: str):
        self.__param1 = v

    @property
    def param2(self) -> str:
        return self.__param2

    @param2.setter
    def param2(self, v: str):
        self.__param2 = v

    def set_param(self, v: str):
        self.param1 = v


# =======================================================================
# add CONNECT params
class DbBaseConn03(DbBaseConn02, abc.ABC):

    # -------------------------------------------------------------------
    def __init__(self, desc_db: str = None, info_msg: str = None):
        super().__init__(desc_db, info_msg)

    # -------------------------------------------------------------------
    def set_connect(self, v):
        self.conn = v

    def get_conn(self):
        if not self.is_error():
            return self.conn
        return None

    @property
    def is_connected(self) -> bool:
        if self.conn and not self.is_error():
            return True
        return False

    # -------------------------------------------------------------------
    def commit(self):
        pass

    # -------------------------------------------------------------------
    def rollback(self):
        pass

    # -------------------------------------------------------------------
    @abc.abstractmethod
    def get_type(self) -> str:
        """ get_type """

    # -------------------------------------------------------------------
    @abc.abstractmethod
    def get_table_parts(self, schema: str, name: str) -> int:
        """ get_table_parts """

    # -------------------------------------------------------------------
    @abc.abstractmethod
    def check_connect(self, p_desc: str = ''):
        pass

    # -------------------------------------------------------------------
    @abc.abstractmethod
    def col_to_insert(self, p_name: str, p_cols: list) -> str:
        pass

    # -------------------------------------------------------------------
    @abc.abstractmethod
    def get_sql_fields(self, sql_text: str) -> list:
        pass


# =======================================================================
class DbBaseConn(DbBaseConn03):

    # -------------------------------------------------------------------
    def __init__(self, desc_db: str = None, info_msg: str = None):
        super().__init__(desc_db, info_msg)

    # -------------------------------------------------------------------
    def set_connect(self, conn):
        super().set_connect(conn)
        logging.debug(f'Create connect: {self.id} {self.info}')

    # -------------------------------------------------------------------
    def set_conn_error(self, error_message):
        super().set_conn_error(error_message)
        logging.error(f'Error create connect {self.id}: {self.info}')
        logging.error(error_message)

    # -------------------------------------------------------------------
    def commit(self):
        if self.conn:
            self.conn.commit()

    # -------------------------------------------------------------------
    def rollback(self):
        if self.conn:
            self.conn.rollback()

    # -------------------------------------------------------------------
    def disconnect(self):
        if self.conn:
            self.conn.close()
            logging.debug(f'Connect {self.id} closed')

        super().disconnect()

    # -------------------------------------------------------------------
    def get_db_cursor(self):
        """ get cursor """
        if not self.conn or self.is_error():
            return None

        c = self.conn.cursor()
        return c

    # -------------------------------------------------------------------
    def run_sql(self, sql, data: tuple = None):
        c = self.get_db_cursor()
        try:
            if data is not None:
                c.execute(sql, data)
            else:
                c.execute(sql)
        finally:
            c.close()

    # -------------------------------------------------------------------
    def run_sql_return_id(self, sql, data: tuple = None) -> int:
        c = self.get_db_cursor()
        try:
            if data is not None:
                c.execute(sql, data)
            else:
                c.execute(sql)
            res = c.fetchone()[0]
        finally:
            c.close()
        return res

    # -------------------------------------------------------------------
    def get_data_1st(self, sql, data: tuple = None):
        c = self.get_db_cursor()
        res = None
        try:
            if data is not None:
                c.execute(sql, data)
            else:
                c.execute(sql)
            records = c.fetchall()
            if len(records) > 0:
                if len(records[0]) > 0:
                    res = records[0][0]
        finally:
            c.close()
        return res

    # -------------------------------------------------------------------
    def get_data_all(self, sql, data: tuple, field_names):
        c = self.get_db_cursor()
        try:
            if data is not None:
                c.execute(sql, data)
            else:
                c.execute(sql)
            records = c.fetchall()
            res = []
            for rec in records:
                r = {}
                i = 0
                for field in field_names:
                    r[field] = rec[i]
                    i += 1
                res.append(r)

        finally:
            c.close()
        return res

    # -------------------------------------------------------------------
    @staticmethod
    def get_json(cur, one=False):
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
        return (r[0] if r else None) if one else r

    # -------------------------------------------------------------------
    def get_table_parts(self, schema: str, name: str) -> int:
        return 0

    # -------------------------------------------------------------------
    def get_sql_fields(self, sql_text: str) -> list:
        return []

    # -------------------------------------------------------------------
    def col_to_insert(self, p_name: str, p_cols: list) -> str:
        return ''

    def check_connect(self, p_desc: str = ''):
        pass

    def get_type(self) -> str:
        return ''


# =====================================================
# == Эти функции нужно переопределить в наследниках
# =====================================================

def create_connect_db(p_one_db: dict, p_dir: str = '') -> DbBaseConn:
    return DbBaseConn(p_one_db[0], p_dir)
