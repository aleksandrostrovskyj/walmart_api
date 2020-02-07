import json
import pickle
import logging
import requests
import urllib.parse
from pathlib import Path
from settings import config
from datetime import datetime

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

BASE_DIR = Path(__file__).parent


class ResponseHandler:
    """
    Class implementing a response processing decorator
    """
    @classmethod
    def handler(cls, func):
        def wrapper(*args, **kwargs):
            try:
                logging.info('Make request.')
                response = func(*args, **kwargs)
                response.raise_for_status()
            except requests.exceptions.HTTPError as http_error:
                logging.warning(f'{http_error} - Issue with request')
            except Exception as exc:
                logging.warning(f'{exc} - issue in program')
            else:
                return response
        return wrapper


class WalmartBase:
    """
    Base class
    """
    config = config['walmart_api']
    headers = {
        'WM_SVC.NAME': 'Walmart Marketplace',
        'WM_QOS.CORRELATION_ID': '123456abcdef'
    }
    main_url = 'https://marketplace.walmartapis.com{}'

    @classmethod
    def local_token(cls) -> str:
        """
        Method to load token from local pickle file

        # if file is missed or token is expired - call 2 methods
        # to request token via API and store in the local file

        :return: API token from local file
        """
        if not Path(BASE_DIR / 'token.pickle').is_file():
            logging.info('Local token missed.')
            cls.save_token(cls.request_token())

        with open(BASE_DIR / 'token.pickle', 'rb') as f:
            token_data = pickle.load(f)

        logging.info('Check token lifetime...')
        if (datetime.timestamp(datetime.now()) - token_data['timestamp']) > 860:
            logging.info('Token has been expired.')
            cls.save_token(cls.request_token())

        return token_data['access_token']

    @classmethod
    @ResponseHandler.handler
    def request_token(cls) -> 'response obj':
        """
        API method to get token from Walmart
        :return: response object that should be processed via save_token static method
        """
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            **cls.headers
        }

        data = {
            'grant_type': 'client_credentials'
        }

        logging.info('Request for the new token.')
        return requests.post(url=cls.main_url.format('/v3/token'),
                             data=data,
                             headers=headers,
                             auth=(cls.config['client_id'], cls.config['client_secret']))

    @staticmethod
    def save_token(response) -> bool:
        """
        Method to save token to the local .pickle file
        :param response: response with token data that received via request_token method
        :return: True
        """
        token_data = response.json()
        token_data.update({'timestamp': datetime.timestamp(datetime.now())})

        with open(BASE_DIR / 'token.pickle', 'wb') as f:
            pickle.dump(file=f, obj=token_data)

        logging.info('Token has been saved to the file.')
        return True

    def sign_request(func):
        """
        Decorator to sign request with token and needed headers
        :return:
        """
        def sign(self, *args, **kwargs):
            logging.info('Try to sign request with auth token.')
            logging.info('Load token from local file.')
            headers = {
                'WM_SEC.ACCESS_TOKEN': self.local_token() or self.request_token(),
                'WM_CONSUMER.CHANNEL.TYPE': '0f3e4dd4-0514-4346-b39d-af0e00ea066d',
                'Accept': 'application/json',
                **self.headers
            }
            logging.info('Token added to the request.')
            return func(self, *args,
                        url=self.main_url.format(kwargs.pop('api_url')),
                        headers=headers,
                        auth=(self.config['client_id'], self.config['client_secret']),
                        **kwargs)
        return sign

    @sign_request
    @ResponseHandler.handler
    def api_get(self, *args, **kwargs):
        """
        Base method to send GET-requests to the Walmart API
        :param args:
        :param api_url:
        :param kwargs:
        :return: response object
        """
        return requests.get(*args, **kwargs)


class WalmartOrders(WalmartBase):
    """
    Class that can realize methods per each API entity on Walmart
    """
    def orders_list(self, params: dict):
        """
        List all orders
        possible parameters
            # sku
                : A seller-provided Product ID
                : string
                : optional
            # customerOrderId
                : The customer order ID
                : string
                : optional
            # purchaseOrderId
                : The purchase order ID. One customer may have multiple purchase orders.
                : string
                : optional
            # status
                : Status may be specified to return orders of that type.
                  Valid statuses are Created, Acknowledged, Shipped, and Cancelled.
                : string
                : optional
            # createdStartDate
                : Start Date for querying all purchase orders after that date.
                  Use one of the following formats, based on UTC, ISO 8601 ('YYYY-MM-DD')
                : string
                : required
            # createdEndDate
                : Limits orders returned to those created before this createdEndDate.
                : string
                : optional
            # fromExpectedShipDate
                : Limits orders returned to those that have orderLines with an expected
                  ship date after this fromExpectedShipDate.
                :
                : optional
            # toExpectedShipDate
                : Limits orders returned to those that have orderLines with an expected
                  ship date before this toExpectedShipDate.
                : string
                : optional
            # limit
                : Limits orders returned to those that have orderLines with an expected ship date
                  before this toExpectedShipDate.
                : string
                : optional

        :return: response object
        """
        while True:
            response = self.api_get(api_url='/v3/orders', params=params)
            yield response.json()
            # Url params of the next page
            next_cursor = response.json()['list']['meta']['nextCursor']
            # Check if next page exist
            if not next_cursor:
                break

            params = {k: v[0] for k, v in urllib.parse.parse_qs(next_cursor[1:]).items()}
            print(params)


def test_orders_list(params):
    """
    Test launch
    """
    walmart = WalmartOrders()
    data = walmart.orders_list(params=params)
    json_data = json.dumps(data.json(), indent=3)

    with open('walmart2.json', 'w') as f:
        f.writelines(json_data)

    with open('walmart2.pickle', 'wb') as f:
        pickle.dump(file=f, obj=data)

