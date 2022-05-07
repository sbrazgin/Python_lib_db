# author: Brazgin Sergey / sbrazgin@gmail.com / 03.2021
import psycopg2
import logging
import db_cursor


# ================================================================================
class PgCursor(db_cursor.DbBaseCurr):

    # -------------------------------------------------------------------
    def __init__(self, conn, sql=''):
        super().__init__(conn, sql)

    # -------------------------------------------------------------------
    def open_sql_cursor(self, bind_list=None):
        """ get cursor """
        super().clear()
        c = self.get_cursor()
        if c is None:
            return None

        try:
            if bind_list is not None:
                c.execute(super().get_sql(), bind_list)
            else:
                c.execute(super().get_sql())

            super().set_open(True)

            return c
        except psycopg2.Error as err:
            super().set_error(str(err))
            return None

    # -------------------------------------------------------------------
    # для совместимости: get_sql_cursor = open_sql_cursor
    def get_sql_cursor(self, bind_list=None):
        return self.open_sql_cursor(bind_list)

    # -------------------------------------------------------------------
    def exec_sql(self, bind_list=None):
        self.open_sql_cursor(bind_list)

    # -------------------------------------------------------------------
    def get_sql_data_1st(self, bind_list=None):
        c = self.open_sql_cursor(bind_list)

        if c is None:
            return None

        for row in c:
            return row[0]

        return None

    # -------------------------------------------------------------------
    def get_sql_data_1st_row(self, bind_list=None):
        c = self.open_sql_cursor(bind_list)

        if c is None:
            return None

        for row in c:
            return row

        return None


# ================================================================================
class PgCursorLogging(PgCursor):

    # -------------------------------------------------------------------
    def __init__(self, conn, sql, message_writer):
        super().__init__(conn, sql)
        self.__msg_writer = message_writer

    # -------------------------------------------------------------------
    def open_sql_cursor(self, bind_list=None):
        """ get cursor """
        c = super().open_sql_cursor(bind_list)
        if c is None or super().is_error():
            err_msg = super().get_error() + ' [' + super().get_sql() + ']'
            logging.error(f'Error execute SQL: {err_msg}')
            self.__message_write3("Error execute SQL", err_msg)
        return c

    # -------------------------------------------------------------------
    def __message_write(self, s1, s2):
        if self.__msg_writer is not None:
            self.__msg_writer.add_message(s1, s2)

    # -------------------------------------------------------------------
    def __message_write3(self, s1, s2):
        if self.__msg_writer is not None:
            if s2 is not None and 'pg_blocking_pids' in s2:
                pass
            else:
                self.__msg_writer.add_message3(s1, super().get_db_desc(), s2)
