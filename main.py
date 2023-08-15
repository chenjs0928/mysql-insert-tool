import configparser
import pymysql
import pandas as pd


def read_db_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config['database'].get("host")
    port = int(config['database'].get("port"))
    user = config['database'].get("user")
    password = config['database'].get("password")
    database = config['database'].get("database")
    config = {"host": host, "user": user, "password": password, "database": database, "port": port}
    return config


def get_table_name():
    config = configparser.ConfigParser()
    config.read('config.ini')
    name = config['database'].get("table")
    return name


def read_insert_data(table_name):
    filename = "{}.xlsx".format(table_name)
    df = pd.read_excel(filename, sheet_name=table_name)
    return df


def insert_data(cursor, table_name, data):
    print(data)
    columns_list = list(data.keys())
    values = list(data.values())
    sql_format = "INSERT INTO {}({}) VALUES ({});"
    columns = ','.join(columns_list)
    placeholders = ','.join(['%s' for _ in columns_list])
    sql = sql_format.format(table_name, columns, placeholders)
    print(sql)
    cursor.execute(sql, values)


if __name__ == '__main__':
    db_config = read_db_config()
    tb_name = get_table_name()
    insert_df = read_insert_data(tb_name)
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    insert_df.apply(lambda row: insert_data(cursor, tb_name, row.dropna().to_dict()), axis=1)
    conn.commit()
