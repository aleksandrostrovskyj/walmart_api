import logging
from datetime import date, timedelta
from walmart import WalmartOrders
from parsers import parse_walmart_order
from db import Mysql, insert_order_data, delete_order_general_data

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def main(start_date):
    walmart = WalmartOrders()
    params = {
        'createdStartDate': start_date
    }
    for orders in walmart.orders_list(params):

        order_general = []
        order_charges = []
        order_refunds = []

        for order in orders['list']['elements']['order']:
            a = parse_walmart_order(order)
            order_general.extend(a[0])
            order_charges.extend(a[1])
            order_refunds.extend(a[2])

        order_general_to_insert = str(order_general).strip('[]')
        order_charges_to_insert = str(order_charges).strip('[]')
        order_refunds_to_insert = str(order_refunds).strip('[]')

        with Mysql() as db:
            cursor = db.cursor()
            cursor = delete_order_general_data(cursor, start_date)
            cursor = insert_order_data(cursor, order_general_to_insert, 'walmart_order_general_data')
            cursor = insert_order_data(cursor, order_charges_to_insert, 'walmart_order_charges')
            cursor = insert_order_data(cursor, order_refunds_to_insert, 'walmart_order_refund_data')
            logging.info('Commit changes')
            db.commit()
            cursor.close()
        logging.info('Finish.')


if __name__ == '__main__':
    start_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    main(start_date)
