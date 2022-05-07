# author: Brazgin Sergey / sbrazgin@gmail.com / 03.2021
import cx_Oracle
import db_cursor


# ================================================================================
class OraCursor(db_cursor.DbBaseCurr):

    # -------------------------------------------------------------------
    # bind_list = type List! ([])
    def open_sql_cursor(self, bind_list: list = None, p_count_rows: int = None):
        """ get cursor """
        """ get cursor """
        self.clear()
        c = self.get_cursor()
        if c is None:
            return None

        if p_count_rows is not None and p_count_rows > 0:
            c.arraysize = p_count_rows

        try:
            if bind_list is not None:
                c.execute(super().get_sql(), tuple(bind_list))
            else:
                c.execute(super().get_sql())

            self.set_open(True)

            return c
        except cx_Oracle.DatabaseError as err:
            super().set_error(err)
            return None

    # -------------------------------------------------------------------
    # для совместимости: get_sql_cursor = open_sql_cursor
    def get_sql_cursor(self, bind_list=None, p_count_rows=None):
        return self.open_sql_cursor(bind_list, p_count_rows)

    # -------------------------------------------------------------------
    # для совместимости: exec_sql = open_sql_cursor
    def exec_sql(self, bind_list=None):
        self.open_sql_cursor(bind_list)

    # -------------------------------------------------------------------
    def get_sql_data_1st(self, bind_list=None, p_count_rows=None):
        c = self.open_sql_cursor(bind_list, p_count_rows)

        if c is None:
            return None

        for row in c:
            return row[0]

        return None


# ================================================================================
class OraOpenCursor(OraCursor):

    # -------------------------------------------------------------------
    def __init__(self, conn, sql='', bind_list=None):
        super().__init__(conn, sql)
        self.exec_sql(bind_list)
