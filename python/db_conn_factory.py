# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020

import abc_conn
import ora_connection
import pg_connection
import csv_connection


# -------------------------------------------------------------------
class ConnFactory:
    def __init__(self):
        self._dbs = []

    # -------------------------------------------------------------------
    def register_database(self, db):
        self._dbs.append(db)

    # -------------------------------------------------------------------
    def get_database(self, tp: dict):
        if abc_conn.DB_TYPE in tp:
            s = tp[abc_conn.DB_TYPE].lower()
        else:
            s = 'postgresql'
        return self.get_database_s(s)

    # -------------------------------------------------------------------
    def get_database_s(self, s: str):
        for i in self._dbs:
            if i.check_type(s):
                return i.create_connect_db

        raise ValueError('Type of database!')


# -------------------------------------------------------------------
factory = ConnFactory()
factory.register_database(ora_connection)
factory.register_database(pg_connection)
factory.register_database(csv_connection)
