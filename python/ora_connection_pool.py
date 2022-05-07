# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020

import cx_Oracle
import ora_connection


# =======================================================================
class OracleConnPool(object):

    # -------------------------------------------------------------------
    def __init__(self, username, password, hostname, port, service_name, p_min, p_max):
        self.__username = username
        self.__password = password
        self.__hostname = hostname
        self.__port = port
        self.__service_name = service_name
        self.__min = p_min
        self.__max = p_max
        self.__pool = None

    # -------------------------------------------------------------------
    def connect(self, type_conn='SERVICE_NAME'):
        """ Connect to DB """
        if type_conn == 'SERVICE_NAME':
            dsb_tns = cx_Oracle.makedsn(self.__hostname,
                                        self.__port,
                                        service_name=self.__service_name)
        else:
            dsb_tns = cx_Oracle.makedsn(self.__hostname,
                                        self.__port,
                                        sid=self.__service_name)

        self.__pool = cx_Oracle.SessionPool(user=self.__username,
                                            password=self.__password,
                                            dsn=dsb_tns,
                                            min=self.__min,
                                            max=20, # self.__max,
                                            increment=1,
                                            encoding="UTF-8")

    # -------------------------------------------------------------------
    def get_conn(self):
        conn = self.__pool.acquire()
        # print(f'Opened {self.__pool.opened()}')
        return conn

    # -------------------------------------------------------------------
    def release_conn(self, conn):
        self.__pool.release(conn)

    # -------------------------------------------------------------------
    def close(self):
        self.__pool.close()

    # -------------------------------------------------------------------
    # -- далее работа с ora_connection.OracleConn
    # -------------------------------------------------------------------
    def get(self):
        conn = self.get_conn()
        ora_conn = ora_connection.OracleConn(self.__username, self.__password,
                                             self.__hostname, self.__port,
                                             self.__service_name)
        ora_conn.set_connect(conn)

        return ora_conn

    # -------------------------------------------------------------------
    def release(self, conn):
        c = conn.get_conn()
        self.release_conn(c)
