import logging
from pathlib import Path
from walmart import Walmart
from datetime import datetime, date, timedelta
from parsers import parse_walmart_order
from db import Mysql, insert_order_data, delete_order_general_data, insert_recon_data, delete_recon_data

import csv
import io
import zipfile

BASE_DIR = Path(__file__).parent

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO, filename=BASE_DIR / 'walmart.log')


def main_orders(start_date: str):
    """
    Program to extract order data via Walmart Marketplace API
    :param start_date: date from orders was cretaed;
    :return: None
    """
    walmart = Walmart()
    params = {
        'createdStartDate': start_date,
        'limit': '200'
    }
    order_general = []
    order_charges = []
    order_refunds = []

    for response_page in walmart.orders_list(params):
        orders = response_page.json()
        for order in orders['list']['elements']['order']:
            parsed = parse_walmart_order(order)
            order_general.extend(parsed[0])
            order_charges.extend(parsed[1])
            order_refunds.extend(parsed[2])

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


def main_recon_report():
    """
    Program for receiving Walmart Marketplace reconciliation report
    :return: None
    """
    def unpack_zip(content):
        """
        Function to retrieve csv-report from zip object
        :param content: requests.Response().content that returns via Walmart().get_recon_report() method
        :return: csv string
        """
        zip_data = io.BytesIO()
        zip_data.write(content)
        zip_file = zipfile.ZipFile(zip_data)
        return zip_file.open(zip_file.namelist()[0]).read().decode()

    def convert_date(date_element):
        return datetime.strptime(date_element, '%m/%d/%Y').date().strftime('%Y-%m-%d')

    walmart = Walmart()
    report_dates = walmart.available_recon_reports()

    for available_date in report_dates.json()['availableApReportDates']:
        response = walmart.get_recon_report(available_date)
        # Retrieve data from zip content
        report = unpack_zip(response.content)
        # Covert to python csv object
        csv_report = csv.reader(io.StringIO(report), delimiter=',')
        # Add last column - report_available_date
        data = [[*each, available_date] for each in csv_report][1:]
        # Convert date column
        for each in data:
            each[6] = convert_date(each[6])
        # Convert to str to use in SQL string
        data_to_insert = str([tuple(each) for each in data]).strip('[]')
        with Mysql() as db:
            cursor = db.cursor()
            cursor = delete_recon_data(cursor, available_date)
            cursor = insert_recon_data(cursor, data_to_insert)
            logging.info('Commit changes')
            db.commit()
            cursor.close()
    logging.info('Finish.')


if __name__ == '__main__':
    start_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    main_orders(start_date)
    main_recon_report()
