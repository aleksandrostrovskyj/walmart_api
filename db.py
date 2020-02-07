import logging
import mysql.connector
from settings import config

config = config['mysql']


class Mysql:
    def __enter__(self):

        logging.info('Initialize connection to database')
        self.conn = mysql.connector.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'])
        logging.info('Connection is ready')
        return self.conn

    def __exit__(self, *exc_info):
        if exc_info[0]:
            logging.warning('Issue with database connection. Rollback')
            self.conn.rollback()
            logging.exception('Exception details:')
        self.conn.close()


def delete_order_general_data(cursor, date_from: 'date string'):
    # Delete old data
    sql_delete_query = f"""
        delete from walmart.walmart_order_general_data
        where DATE(order_date) >= '{date_from}'
    """
    logging.info('Execute delete query.')
    cursor.execute(sql_delete_query)
    return cursor


def insert_order_data(cursor, data_to_insert, table):
    sql_insert_query = f"""
        insert ignore into walmart.{table}
        values {data_to_insert}
    """
    logging.info('Execute insert query.')
    cursor.execute(sql_insert_query)
    return cursor

