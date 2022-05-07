# Python_lib_db

Библиотеки на Python для работы с базами данных

Пример использования:

# --------------------------------------------------------------------------------------
def read_one_db(p_one_db, o_store):
    # connect to DB
    conn = ora_connection.create_connect_db(p_one_db)

    v_db_host = p_one_db["host"]
    print("host:", v_db_host)

    if conn is not None:
        try:
            # report 1
            o_c = ora_cursor.OraCursor(conn, V_SQL_1)
            c = o_c.get_sql_cursor()
            if c is None:
                return
            else:
                try:
                    ........
                finally:
                    o_c.close()

        finally:
            conn.close()
# --------------------------------------------------------------------------------------
