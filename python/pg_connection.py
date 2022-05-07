# author: Brazgin Sergey / sbrazgin@gmail.com / 03.2021
import psycopg2 as pg
import logging
import json
import conf_utils
import abc_conn

NAME_TYPE_1 = 'postgresql'
NAME_TYPE_2 = 'pg'
NAME_TYPE_3 = 'postgres'


class PgConnection00(abc_conn.DbBaseConn):

    # -------------------------------------------------------------------
    def __init__(self, username, password, hostname, port, db_name):
        self.__username = username
        self.__password = password
        self.__hostname = hostname
        self.__port = port
        self.__db_name = db_name

        db_desc = f'PostgreSql DB: {hostname} - {db_name}'
        info_msg = f'PostgreSql DB: hostname={hostname}, port={port}, db_name={db_name}, ' \
                   f'username={username}'

        super().__init__(db_desc, info_msg)

    # -------------------------------------------------------------------
    def connect(self):
        """ Connect to PG DB """
        conn_str = "host={0:s} user={1:s} port={2:s} dbname={3:s} password={4:s}".\
            format(self.__hostname, self.__username, str(self.__port), self.__db_name,
                   self.__password)
        try:
            conn = pg.connect(conn_str)
            super().set_connect(conn)
        except pg.Error as err:
            super().set_conn_error(str(err))

        return super().get_conn()

    # -------------------------------------------------------------------
    def run_sql_many(self, sql, data: tuple):
        c = self.get_db_cursor()
        try:
            c.executemany(sql, data)
        finally:
            c.close()

    # -------------------------------------------------------------------
    def check_connect(self, p_desc: str = ''):
        s = self.get_data_1st('select current_database()')
        logging.info(f'Connected {p_desc} OK, db_name: ' + s)

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
            # values = values + ':' + col
            values = values + '%s'

        res = "insert into " + p_name + '(' + columns + ") values (" + values + ")"
        return res

    # -------------------------------------------------------------------
    def get_type(self) -> str:
        return NAME_TYPE_1

    # -------------------------------------------------------------------
    def get_table_parts(self, schema: str, name: str) -> int:
        return 0

    # -------------------------------------------------------------------
    def get_sql_fields(self, sql_text: str) -> list:
        return []


# =========================================================================
# Class extended with logging func
class PgConnection0(PgConnection00):

    # -------------------------------------------------------------------
    def __init__(self, username, password, hostname, port, db_name):
        super().__init__(username, password, hostname, port, db_name)
        self.__msg_writer = None

    # -------------------------------------------------------------------
    def set_logger(self, s):
        self.__msg_writer = s

    # -------------------------------------------------------------------
    def connect(self):
        c = super().connect()
        if c is None or super().is_error():
            self.__message_write("Error connect to DB", super().get_error())
        return c

    # -------------------------------------------------------------------
    def __message_write(self, s1, s2):
        if self.__msg_writer is not None:
            self.__msg_writer.add_message3(s1, super().desc, s2)


# =========================================================================
# Class extended with logging func
class PgConnection(PgConnection0):
    # -------------------------------------------------------------------
    def __init__(self, p_one_db: dict, p_dir: str = ''):
        # mandatory
        v_db_host = p_one_db[abc_conn.DB_HOST]
        v_db_user = p_one_db[abc_conn.DB_USER]
        v_db_name = p_one_db[abc_conn.DB_DB]

        # optional
        v_db_user_pass = ''
        if abc_conn.DB_PASS_FILE in p_one_db:
            v_db_user_pass = conf_utils.read_1st_line(p_dir + p_one_db[abc_conn.DB_PASS_FILE])
        elif abc_conn.DB_PASS in p_one_db:
            v_db_user_pass = p_one_db[abc_conn.DB_PASS]

        v_db_port = 5432
        if abc_conn.DB_PORT in p_one_db:
            v_db_port = p_one_db[abc_conn.DB_PORT]

        super().__init__(v_db_user, v_db_user_pass, v_db_host, v_db_port, v_db_name)

        if abc_conn.DB_TYPE in p_one_db:
            v = p_one_db[abc_conn.DB_TYPE]
            self.set_param(v)


# =========================================================================
def create_connect_db(p_one_db: dict, p_dir: str = '') -> PgConnection:
    conn = PgConnection(p_one_db, p_dir)
    conn.connect()

    return conn


# =========================================================================
def create_connect_db_file(p_file: str, p_dir: str = ''):
    with open(p_dir + p_file) as json_data_file:
        config_data = json.load(json_data_file)
        db_params = config_data[abc_conn.DB_JSON_NAME]

    conn = create_connect_db(db_params, p_dir)
    return conn


# =========================================================================
def create_connect_db_logging(p_one_db, message_writer):
    conn = PgConnection(p_one_db)
    conn.set_logger(message_writer)

    conn.connect()

    return conn


# =========================================================================
# class for iterate on all DB with ALL tests
class PgConnIterator(object):

    # ------------------------------------------------------------------------
    # file_name - имя json для загрузки списка БД
    # p_messages - класс логирования, должен содержать метод add_message(text1, text2)
    def __init__(self, file_name, p_messages):
        self.__file_name = file_name
        self.__messages = p_messages

        with open(self.__file_name) as json_data_file:
            self.__all_json = json.load(json_data_file)

        self.__list_db = self.__all_json[abc_conn.DB_JSON_LIST]

    # ------------------------------------------------------------------------
    #  на вход подается класс который вызывают на каждое подключение
    #  вызывается метод check_db_tests(connection)
    def db_iterate(self, p_pg_tests):
        for i in self.__list_db:
            logging.info(f'Current database: "{i}"')
            p_one_db = self.__all_json[i]

            # connect to DB
            l_conn = create_connect_db_logging(p_one_db, self.__messages)
            try:
                # call check_db_tests in any case!
                p_pg_tests.check_db_tests(l_conn, p_one_db)
            finally:
                if l_conn is not None:
                    l_conn.close()


# =========================================================================
def check_type(p_type: str) -> bool:
    if p_type == NAME_TYPE_1 or p_type == NAME_TYPE_2 or p_type == NAME_TYPE_3:
        return True
    return False
