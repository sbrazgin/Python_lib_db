
# author: Brazgin Sergey / sbrazgin@gmail.com / 01-02.2020
import json
import logging
import conf_utils
import abc_conn
import cx_Oracle
import ora_cursor

NAME_TYPE_1 = 'oracle'
NAME_TYPE_2 = 'ora'


# =======================================================================
class OracleConn00(abc_conn.DbBaseConn):

    # -------------------------------------------------------------------
    def __init__(self, username, password, hostname, port, service_name, threaded=False):
        self.__username = username
        self.__password = password
        self.__hostname = hostname
        self.__port = port
        self.__service_name = service_name
        self.__threaded = threaded

        db_desc = f'Oracle DB: {hostname} - {service_name}'
        info_msg = f'Oracle DB: hostname={hostname}, port={port}, service_name={service_name}, ' \
                   f'username={username}'  # , password={password}
        super().__init__(db_desc, info_msg)

    # -------------------------------------------------------------------
    def connect(self, type_conn='SERVICE_NAME'):
        """ Connect to DB """
        try:
            if type_conn == 'SERVICE_NAME':
                dsb_tns = cx_Oracle.makedsn(self.__hostname,
                                            self.__port,
                                            service_name=self.__service_name)
            else:
                dsb_tns = cx_Oracle.makedsn(self.__hostname,
                                            self.__port,
                                            sid=self.__service_name)

            conn = cx_Oracle.connect(user=self.__username, password=self.__password, dsn=dsb_tns,
                                     threaded=self.__threaded)
            super().set_connect(conn)
        except cx_Oracle.DatabaseError as err:
            super().set_conn_error(str(err))

        return super().get_conn()

    # -------------------------------------------------------------------
    def check_connect(self, p_desc: str = ''):
        try:
            s = self.get_data_1st('select ora_database_name from dual')
            logging.info(f'Connected {p_desc} OK, db_name: ' + s)
        except cx_Oracle.DatabaseError as err:
            logging.error(err)
            super().set_conn_error(str(err))

    # -------------------------------------------------------------------
    def col_to_insert(self, p_name: str, p_cols: list) -> str:
        columns = ''
        values = ''
        i = 0
        for col in p_cols:
            i += 1
            if i > 1:
                columns = columns + ','
                values = values + ','
            columns = columns + col
            values = values + ':' + col

        res = "insert into " + p_name + '(' + columns + ") values (" + values + ")"
        return res

    # -------------------------------------------------------------------
    def get_type(self) -> str:
        return NAME_TYPE_1

    # -------------------------------------------------------------------
    def get_table_parts(self, schema: str, name: str) -> int:
        res = 0
        sql_text = "select count(*) as CNT from all_tab_partitions where table_owner = '" + \
                   schema + "' " + \
                   "and table_name = '" + name + "' "
        cur = ora_cursor.OraCursor(self, sql_text)
        try:
            cur_ora = cur.open_sql_cursor()

            if cur_ora is None or cur.is_error():
                raise Exception(cur.get_error())

            for c in cur_ora:
                res = c[0]
        finally:
            cur.close()

        return res

    # -------------------------------------------------------------------
    def get_sql_fields(self, sql_text: str) -> list:
        cur = ora_cursor.OraCursor(self, sql_text)

        try:
            cur_ora = cur.open_sql_cursor()

            if cur_ora is None or cur.is_error():
                raise Exception(cur.get_error())

            c = []
            for col in cur_ora.description:
                c.append(col[0])

            return c
        finally:
            cur.close()


# =======================================================================
class OracleConnJson(OracleConn00):

    # -------------------------------------------------------------------
    def __init__(self, js):
        l_username = js.get_db_user()
        l_password = js.get_db_pass()
        l_hostname = js.get_db_host()
        l_port = js.get_db_port()
        l_service_name = js.get_db_service()
        super().__init__(l_username, l_password, l_hostname, l_port, l_service_name)


def create_conn_dict(usr, password, host, port, srv):
    v = {abc_conn.DB_HOST: host,
         abc_conn.DB_USER: usr,
         abc_conn.DB_SERVICE: srv,
         abc_conn.DB_PORT: port,
         abc_conn.DB_PASS: password}
    return v


# =======================================================================
class OracleConn(OracleConn00):

    # -------------------------------------------------------------------
    def __init__(self, p_one_db: dict, p_dir: str = ''):
        # mandatory
        v_db_host = p_one_db[abc_conn.DB_HOST]
        v_db_user = p_one_db[abc_conn.DB_USER]
        v_service = None
        if abc_conn.DB_SERVICE in p_one_db:
            v_service = p_one_db[abc_conn.DB_SERVICE]
        elif abc_conn.DB_DB in p_one_db:
            v_service = p_one_db[abc_conn.DB_DB]

        # optional
        v_db_port = 1521
        if abc_conn.DB_PORT in p_one_db:
            v_db_port = p_one_db[abc_conn.DB_PORT]

        v_db_user_pass = ''
        if abc_conn.DB_PASS_FILE in p_one_db:
            v_db_user_pass = conf_utils.read_1st_line(p_dir + p_one_db[abc_conn.DB_PASS_FILE])
        elif abc_conn.DB_PASS in p_one_db:
            v_db_user_pass = p_one_db[abc_conn.DB_PASS]

        super().__init__(v_db_user, v_db_user_pass, v_db_host, v_db_port, v_service)

        if abc_conn.DB_TYPE in p_one_db:
            v = p_one_db[abc_conn.DB_TYPE]
            self.set_param(v)

        # logging.info(f"pass=[{v_db_user_pass}]")


# =========================================================================
# static proc for create connect
# p_one_db - list of params
def create_connect_db(p_one_db: dict, p_dir: str = '') -> OracleConn or None:
    if p_one_db is None:
        logging.error("Database params for connect empty!")
        return None

    v_db_host = p_one_db[abc_conn.DB_HOST]
    conn = OracleConn(p_one_db, p_dir)

    try:
        logging.debug('Try to connect ... ')
        # print('Try to connect ... ')
        conn.connect()
        logging.debug('Connected OK')
        # print('Connected OK')
    except cx_Oracle.DatabaseError as e:
        logging.error(e)
        # print(e)
        logging.error(f"Error connection to database! ('{v_db_host}')")
        # print("Error connection to database! (" + v_db_host+") ")
        conn = None

    return conn


# =========================================================================
# static proc for create connect
def create_connect_db_file(p_file: str, p_dir: str = ''):
    with open(p_dir + p_file) as json_data_file:
        config_data = json.load(json_data_file)
        db_params = config_data[abc_conn.DB_JSON_NAME]

    # connect to Oracle
    conn = create_connect_db(db_params, p_dir)
    return conn


# =========================================================================
def check_type(p_type: str) -> bool:
    if p_type == NAME_TYPE_1 or p_type == NAME_TYPE_2:
        return True
    return False
