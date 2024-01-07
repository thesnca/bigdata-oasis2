import asyncio
import json

from nubia import argument
from nubia import command
from termcolor import cprint

from cli.methods.order import check_order_status
from oasis.utils.sdk import charge_client


@command
class Order:
    """
        Order commands
    """

    @command
    @argument('order_id', description='Order uuid', positional=True)
    def query(self, order_id: str):
        """
            Query all suborders of order_id
        """

        base_info, table = asyncio.get_event_loop().run_until_complete(check_order_status(order_id))
        cprint(f'Order Id: {order_id}', 'green')
        cprint(base_info, 'cyan')
        cprint(table, 'white')

    @command
    @argument('suborder_id', description='Suborder uuid')
    def find(self, suborder_id: str):
        """
            Find order info
        """
        res = asyncio.get_event_loop().run_until_complete(charge_client.query_sub_orders(suborder_id))
        cprint(f'Suborder id :{suborder_id}', 'green')
        cprint(json.dumps(res, indent=2), 'white')
