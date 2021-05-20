from contextlib import contextmanager
import time

from typing import Optional

from functools import wraps

@contextmanager
def execution_scope(wait: bool = True, timeout: Optional[int] = None):
    """Provide a transactional scope around a series of operations."""
    session = ExecutionSession(timeout)
    try:
        yield session
    except:
        # detach the session from underlying orders
        session._close()
        raise

    try:
        # submit the orders for execution
        session._submit()

        # wait on the trades to finish before returning
        if wait:
            session._wait()
    except Exception as e:
        print("Rolling back!")
        print(e)
        
        # unwind the trades that have already been executed
        session._rollback()
        raise
    finally:
        # detach the session from underlying orders
        session._close()


def not_closed(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if self._closed:
            raise Exception("Session cannot be closed.")
        else:
            return fn(self, *args, **kwargs)
    return wrapper

class ExecutionSession(object):
    def __init__(self, timeout: Optional[int] = None):
        self._timeout = timeout
        self._orders = []
        self._final_status = None
        self._closed = False

    def get_order_statuses(self):
        if not self._closed:
            return [order.get_status() for order in self._orders]
        else:
            return self._final_statuses

    @not_closed
    def add(self, order):
        self._orders.append(order)
        order.set_session(self)

    @not_closed
    def _close(self):
        if not self._closed:
            self._final_statuses = []
            for order in self._orders:
                order_status = order.get_status()
                self._final_statuses.append(order_status)
                
                order.set_session(None)
            
            self._closed = True

    @not_closed
    def _rollback(self):
        cancelled_orders = []

        for order in self._orders:
            if order.is_submitted():
                # first we have to cancel the order
                order.rollback()
                cancelled_orders.append(order)

        for order in cancelled_orders:
            order.wait_until_closed()

    @not_closed
    def _submit(self):
        for order in self._orders:
            order.submit()

    @not_closed
    def _wait(self):
        if self._closed:
            raise Exception("Session is already closed.")
        for order in self._orders:
            order.wait_until_closed()
