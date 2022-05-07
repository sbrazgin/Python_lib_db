# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020

class DbBaseCurr:

    # -------------------------------------------------------------------
    def __init__(self, conn, sql=''):
        self.__conn = conn
        self.__sql = sql

        self.__curr = None
        self.__opened = False
        self.__last_error = None
        self.clear()

        # read connection
        self.__db_connect = None
        if self.__conn is None:
            self.__last_error = "No connection to DB!"
        else:
            self.__db_connect = conn.get_conn()

    # -------------------------------------------------------------------
    def get_cursor(self):
        """ get cursor """
        if self.__db_connect is None:
            return None

        self.__curr = self.__db_connect.cursor()
        return self.__curr

    # -------------------------------------------------------------------
    def clear(self):
        """  """
        if self.__curr is not None:
            if self.__opened:
                self.__curr.close()
            self.__curr = None
        self.__opened = False
        self.__last_error = None

    # -------------------------------------------------------------------
    def close(self):
        """ Close cursor """
        self.clear()
        self.__conn = None
        self.__db_connect = None
        self.__sql = None

    # -------------------------------------------------------------------
    def set_error(self, err):
        print(err)
        self.__last_error = str(err)
        self.__curr = None
        self.__db_connect.rollback()

    # -------------------------------------------------------------------
    def get_error(self):
        return self.__last_error

    # -------------------------------------------------------------------
    def is_error(self):
        return self.__last_error is not None

    # -------------------------------------------------------------------
    # only for parent
    def set_open(self, status):
        self.__opened = status

    # -------------------------------------------------------------------
    def is_open(self):
        return self.__last_error is None and self.__opened

    # -------------------------------------------------------------------
    def get_cur(self):
        return self.__curr

    # -------------------------------------------------------------------
    def get_sql(self):
        return self.__sql

    # -------------------------------------------------------------------
    def get_db_desc(self):
        if self.__conn is None:
            return ""
        return self.__conn.get_desc()
