from typing import TYPE_CHECKING

# this is necessary to avoid a circular import issue with Account and OrderStatus
if TYPE_CHECKING:
    from cryptomancer.account import Account
    from cryptomancer.exchange_feed import ExchangeFeed

from cryptomancer.execution_handler.execution_session import ExecutionSession
from cryptomancer.execution_handler.order_status import OrderStatus

from typing import Optional

from functools import wraps

import pytz
import datetime
import time


def session_required(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not self._session:
            raise Exception("Order must be attached to execution session")
        else:
            return fn(self, *args, **kwargs)
    return wrapper


class Order:
    def __init__(self, type: str, account: 'Account', exchange_feed: 'ExchangeFeed'):
        self._type = type
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

    def failed(self):
        return self._id == -1

    def cancel(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot cancel non-executed order.")

        if self.failed():
            return

        account = self.get_account()
        return account.cancel_order(self.get_id())

    def is_pending(self) -> bool:
        return (self.get_id() is None)

    def is_submitted(self) -> bool:
        return not self.is_pending()

    def is_closed(self) -> bool:
        if not self.get_id():
            raise Exception("Cannot poll non-executed order.")

        if self.failed():
            return True

        order_status = self.get_status()
        return (order_status.status == 'closed' or order_status.status == 'triggered')

    def wait_until_closed(self, timeout: Optional[float] = None):
        if timeout:
            status = self.get_status()
            start_time = status.created_time
            
        while True:
            if self.is_closed():
                break

            # TODO: Better sleep method
            time.sleep(1)

            if timeout:
                now = pytz.utc.localize(datetime.datetime.utcnow())
                if (now - start_time).seconds > timeout:
                    raise TimeoutError("Order timed out.")

    def get_status(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot poll non-executed order.")
        
        if self.failed():
            status = OrderStatus(order_id = -1,
                            created_time = None,
                            market = self._market,
                            type = self._type,
                            side = self._side,
                            size = self._size,
                            filled_size = 0,
                            status = "closed"
            )

        else:
            account = self.get_account()
            status = account.get_order_status(self.get_id())

        return status

    def submit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError
        