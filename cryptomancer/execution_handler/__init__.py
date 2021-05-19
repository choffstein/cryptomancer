from cryptomancer.account import Account
from cryptomancer.exchange_feed import ExchangeFeed

from cryptomancer.execution_handler.execution_session import ExecutionSession

from typing import Optional

from functools import wraps

def session_required(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self._session:
            raise Exception("Order must be attached to execution session")
        else:
            return fn(self, *args, **kwargs)
    return wrapper


class Order:
    def __init__(self, account: Account, exchange_feed: ExchangeFeed):
        self._account = account
        self._exchange_feed = exchange_feed
        self._session = None
        self._id = None

    def get_account(self):
        return self._account

    def get_exchange_feed(self):
        return self._exchange_feed

    def set_session(self, session: ExecutionSession):
        self._session = session

    def get_id(self):
        return self._id

    def set_id(self, order_id):
        self._id = order_id 

    def submit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def cancel(self):
        raise NotImplementedError

    def is_closed(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError

    def wait_until_closed(self):
        raise NotImplementedError