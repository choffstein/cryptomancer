from contextlib import contextmanager
import time

from typing import Optional

@contextmanager
def execution_scope(wait: bool = True, timeout: Optional[int] = None):
    """Provide a transactional scope around a series of operations."""
    session = ExecutionSession(timeout)
    try:
        yield session
    except:
        # detach the session from underlying orders
        session.close()
        raise

    try:
        # submit the orders for execution
        session.submit()

        # wait on the trades to finish before returning
        if wait:
            session.wait()
    except Exception as e:
        print("Rolling back!")
        print(e)
        
        # unwind the trades that have already been executed
        session.rollback()
        raise
    finally:
        # detach the session from underlying orders
        session.close()


class ExecutionSession(object):
    def __init__(self, timeout: Optional[int] = None):
        self._timeout = timeout
        self._orders = []
        self._final_status = None

    def _order_statuses(self):
        return [order.status() for order in self._orders]

    def add(self, order):
        self._orders.append(order)
        order.set_session(self)

    def close(self):
        self._final_status = []
        for order in self._orders:
            order_status = order.get_status()
            self._final_status.append(order_status)

            order.set_session(None)

    def rollback(self):
        cancelled_orders = []

        for order in self._orders:
            if order.is_submitted():
                # first we have to cancel the order
                order.rollback()
                cancelled_orders.append(order)

        for order in cancelled_orders:
            order.wait_until_closed()

    def submit(self):
        for order in self._orders:
            order.submit()

    def wait(self):
        for order in self._orders:
            order.wait_until_closed()
