# -*- coding:utf-8 -*-

"""
Gate.io Future Trade module.
https://api.gateio.ws

Author: April
Date:   2019/09/05
Email:  xxxx
"""
import copy
import hashlib
import hmac
import json
import time
from urllib.parse import urljoin

import asyncio

from quant.asset import AssetSubscribe, Asset
from quant.const import GATE_FUTURE
from quant.error import Error
from quant.order import TRADE_TYPE_BUY_OPEN, TRADE_TYPE_BUY_CLOSE, TRADE_TYPE_SELL_OPEN, ORDER_TYPE_LIMIT, \
    ORDER_TYPE_MARKET, ORDER_ACTION_BUY, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, Order, ORDER_ACTION_SELL, \
    ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_SUBMITTED
from quant.position import Position
from quant.tasks import SingleTask, LoopRunTask
from quant.utils import logger
from quant.utils.decorator import async_method_locker
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.websocket import Websocket

__all__ = ("GateFutureRestAPI", "GateFutureTrade", )


class GateFutureRestAPI:
    """ Gate.io future REST API client.

        Attributes:
            host: HTTP request host.
            access_key: Account's ACCESS KEY.
            secret_key Account's SECRET KEY.
    """

    def __init__(self, host, access_key, secret_key):
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_user_account(self):
        # 获取账户信息
        uri = "/api/v4/futures/accounts"
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_position(self, instrument_id):
        uri = '/api/v4/futures/positions/%s' % instrument_id
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_order_info(self, order_id):
        uri = '/api/v4/futures/orders/{order_id}'.format(order_id=order_id)
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def create_order(self, instrument_id, trade_type, price, size, order_type=ORDER_TYPE_LIMIT):
        """

        :param instrument_id:  合约id:如BTC_USD
        :param trade_type: 1 开多 / 2 开空 / 3 平多 / 4 平空
        :param price: 价格，数字，如果等于0表示是市价单
        :param size:数量，正整数
        :return:
        """

        uri = '/api/v4/futures/orders'
        size = int(size)
        if trade_type == TRADE_TYPE_BUY_OPEN:
            size = abs(size)
            text = 't-1'
        elif trade_type == TRADE_TYPE_BUY_CLOSE:
            size = abs(size)
            text = 't-4'
        elif trade_type == TRADE_TYPE_SELL_OPEN:
            size = -abs(size)
            text = 't-2'
        else:
            size = -abs(size)
            text = 't-3'

        body = {
            'contract': instrument_id,
            'size': size,
            'price': str(price),
            'text': text,
            'iceberg': 0,
            'tif': 'gtc'
        }

        if order_type == ORDER_TYPE_MARKET:
            body['price'] = '0'
            body['tif'] = 'ioc'

        print(body)
        success, error = await self.request('POST', uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, order_no):
        uri = '/api/v4/futures/orders/{order_id}'.format(order_id=order_no)

        success, error = await self.request('DELETE', uri, auth=True)
        return success, error

    async def revoke_orders(self, instrument_id, side=None):
        """
        :param instrument_id: 合约id:如BTC_USD
        :param side: ask/bid/None, 分别代表撤销所有的卖单/买单/卖买单
        :return:
        """
        uri = '/api/v4/futures/orders'
        params = {
            'contract': instrument_id,
        }
        if side:
            params['side'] = side

        success, error = await self.request('DELETE', uri, params=params, auth=True)
        return success, error

    async def get_order_list(self, instrument_id, state):
        """
        :param instrument_id: 合约id:如BTC_USD
        :param status: open/finished
        :return:
        """
        uri = '/api/v4/futures/orders'
        params = {
            'contract':instrument_id,
            'status': state
        }

        success, error = await self.request('GET', uri, params=params, auth=True)
        return success, error

    async def get_contract(self, instrument_id):
        uri = '/api/v4/futures/contracts/{contract}'.format(contract=instrument_id)
        success, error = await self.request('GET', uri)
        return success, error

    async def request(self, method, uri, params=None, body=None, auth=False):

        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        if auth:
            # 生成签名并携带在请求头
            t = int(time.time())
            m = hashlib.sha512()

            payload_string = None
            if body:
                payload_string = json.dumps(body)

            query_string = None
            if params:
                query_string = "&".join(["=".join([str(k), str(v)]) for k, v in params.items()])

            m.update((payload_string or "").encode('utf-8'))

            hashed_payload = m.hexdigest()
            s = '%s\n%s\n%s\n%s\n%s' % (method, uri, query_string or "", hashed_payload, t)

            sign = hmac.new(self._secret_key.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()

            sign_headers = {
                'KEY': self._access_key,
                'Timestamp': str(t),
                'SIGN': sign
            }

            headers.update(sign_headers)

        url = urljoin(self._host, uri)
        _, success, error = await AsyncHttpRequests.fetch(method, url, params=params, data=body, headers=headers, timeout=10)
        print(_, success, error)
        if error:
            return None, error
        return success, None


class GateFutureTrade(Websocket):
    """
    OKEX Future Trade module
    """

    def __init__(self, **kwargs):
        """ 初始化
        """
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.gateio.ws"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://fx-ws.gateio.ws"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = GATE_FUTURE
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._contract_update_callback = kwargs.get('contract_update_callback')
        self._user_id = None

        url = self._wss + "/v4/ws"
        super(GateFutureTrade, self).__init__(url, send_hb_interval=5)
        self.heartbeat_msg = "ping"

        self._assets = {}  # 资产 {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # 订单 {"order_no": order, ... }
        self._position = Position(self._platform, self._account, self._strategy, self._symbol)

        # 初始化 REST API 对象
        self._rest_api = GateFutureRestAPI(self._host, self._access_key, self._secret_key)

        # 初始化资产订阅
        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        # 注册定时任务
        LoopRunTask.register(self._check_position_update, 1)  # 获取持仓

    async def on_event_asset_update(self, asset: Asset):
        """ 资产数据更新回调
        """
        self._assets = asset
        SingleTask.run(self._asset_update_callback, asset)

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    @property
    def position(self):
        return copy.copy(self._position)

    async def _check_position_update(self):
        update = False
        success, error = await self._rest_api.get_position(self._symbol)
        if error:
            return
        if not self._position.utime:  # 如果持仓还没有被初始化，那么初始化之后推送一次
            update = True
            self._position.update()
        size = success['size']
        average_price = float(success['entry_price'])
        liquid_price = float(success["liq_price"])
        if size > 0:
            if self._position.long_quantity != size:
                update = True
                self._position.update(0, 0, size, average_price, liquid_price)
        elif size < 0:
            if self._position.short_quantity != abs(size):
                update = True
                self._position.update(abs(size), average_price, 0, 0, liquid_price)
        elif size == 0:
            if self._position.long_quantity != 0 or self._position.short_quantity != 0:
                update = True
                self._position.update()
        if update:
            await self._position_update_callback(copy.copy(self._position))

    async def connected_callback(self):
        """ 建立连接之后，然后订阅order和position
        """
        # 订阅order

        while True:
            success, error = await self._rest_api.get_user_account()
            if success:
                self._user_id = success['user']
                break
        t = int(time.time())
        order_message = 'channel=%s&event=%s&time=%s' % ('futures.orders', 'subscribe', t)
        sign = hmac.new(self._secret_key, order_message, hashlib.sha512).hexdigest()

        data = {
            'time': t,
            'channel': "futures.orders",
            "event": "subscribe",
            "payload": [self._user_id, self._symbol],
            "auth": {
                "method": "api_key",
                "KEY": self._access_key,
                "SIGN": sign
            }
        }
        await self.ws.send_json(data)

        # 获取持仓
        await self._check_position_update()

        # # 订阅仓位
        # positon_message = 'channel=%s&event=%s&time=%s' % ('futures.position_closes', 'subscribe', t)
        # sign = hmac.new(self._secret_key, positon_message, hashlib.sha512).hexdigest()
        # data = {
        #     'time': t,
        #     'channel': "futures.position_closes",
        #     "event": "subscribe",
        #     "payload": [self._user_id, self._symbol],
        #     "auth": {
        #         "method": "api_key",
        #         "KEY": self._access_key,
        #         "SIGN": sign
        #     }
        # }
        # await self.ws.send_json(data)

    @async_method_locker('process.locker')
    async def process(self, msg):
        channel = msg['channel']
        event = msg['event']
        result = msg['result']
        if channel == "futures.orders":
            # 订单订阅返回消息
            if event == "subscribe":
                error = msg['error']
                if error:
                    if self._init_success_callback:
                        e = Error("subscribe order event error: {}".format(error))
                        SingleTask.run(self._init_success_callback, False, e)
                else:
                    # 获取之前未完成的订单信息并推送
                    result, error = await self._rest_api.get_order_list(self._symbol, open)
                    if error:
                        e = Error("get open orders error: {}".format(error))
                        if self._init_success_callback:
                            SingleTask.run(self._init_success_callback, False, e)
                        return
                    for order_info in result:
                        is_update, order = self._update_order(order_info)
                        if is_update:
                            if self._order_update_callback:
                                SingleTask.run(self._order_update_callback, copy.copy(order))

            # 订单更新推送
            else:
                for data in result:
                    is_update, order = self._update_order(data)
                    if is_update:
                        if order and self._order_update_callback:
                            SingleTask.run(self._order_update_callback, copy.copy(order))
                return

        else:
            logger.warn("unhandle msg:", msg, caller=self)

        # elif channel == 'futures.position_closes':
        #     # 仓位订阅返回消息
        #     if event == "subscribe":
        #         error = msg['error']
        #         if error:
        #             if self._init_success_callback:
        #                 e = Error("subscribe position event error: {}".format(error))
        #                 SingleTask.run(self._init_success_callback, False, e)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """

        :param action: 交易方向 BUY/SELL
        :param price: 价格，数字，如果等于0表示是市价单
        :param quantity: 委托数量(可以是正数，也可以是负数)
        :param order_type:ORDER_TYPE_LIMIT/ORDER_TYPE_MARKET
        :return:
        """
        if int(quantity) > 0:
            if action == ORDER_ACTION_BUY:
                trade_type = "1"
            else:
                trade_type = "3"
        else:
            if action == ORDER_ACTION_BUY:
                trade_type = "4"
            else:
                trade_type = "2"
        result, error = self._rest_api.create_order(self._symbol, trade_type, price, quantity, order_type=order_type)
        if error:
            return None, error
        return result['id'], None

    async def revoker_order(self, *order_nos):
        # 如果传入order_nos为空，即撤销全部委托单
        if len(order_nos) == 0:
            result, error = await self._rest_api.revoke_orders(self._symbol, side=None)
            if error:
                return False, error
            return True, None

        # 如果传入order_nos为一个委托单号，那么只撤销一个委托单
        elif len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(order_nos[0])
            if error:
                # 如果撤销失败，查询订单详情
                # if error.get('label', '') == 'ORDER_FINISHED':
                #     await self.get_order_info(order_nos[0])
                return order_nos[0], error
            else:
                return order_nos[0], None

        # 如果传入order_nos数量大于1，那么就批量撤销传入的委托单
        elif len(order_nos) > 1:
            success, error = [], []
            for order_no in order_nos:
                _, e = await self._rest_api.revoke_order(order_no)
                if e:
                    # if e.get('label') == 'ORDER_FINISHED':
                    #     await self.get_order_info(order_nos[0])
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return success, error

    async def get_order_info(self, order_no):
        order_info, error = await self._rest_api.get_order_info(order_no)
        if error:
            return None, error
        else:
            logger.info(order_info)
            is_update, order = await self._update_order(order_info)
            if is_update:
                if self._order_update_callback:
                    SingleTask.run(self._order_update_callback, copy.copy(order))
            return copy.copy(order), None

    async def _update_order(self, order_info):
        """
        更新订单信息
        :param order_info: 订单信息
        :return:
        """
        order_no = order_info.get('id')
        is_updated = False
        if order_no is None:
            return is_updated, None
        remain = order_info['left']
        quantity = order_info['size']
        finish_as = order_info['finish_as']
        state = order_info['status']
        ctime = order_info['create_tim']
        if state == 'finished':
            if finish_as == 'filled' or finish_as == 'ioc' or finish_as == 'auto_deleveraged':
                status = ORDER_STATUS_FILLED
            elif finish_as == 'cancelled' or finish_as == 'liquidated' or finish_as == 'reduce_only':
                status = ORDER_STATUS_CANCELED
            else:
                logger.error("status error! order_info:", order_info, caller=self)
                return None
        elif state == 'open':
            if quantity != remain:
                if remain != 0:
                    status = ORDER_STATUS_PARTIAL_FILLED
                else:
                    status = ORDER_STATUS_FILLED
            else:
                status = ORDER_STATUS_SUBMITTED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return None

        order = self._orders.get(order_no)
        if order:
            if order.remain != remain:
                is_updated = True
                order.remain = remain
            elif order.status != status:
                is_updated = True
                order.status = status
            elif order.avg_price != order_info['fill_price']:
                is_updated = True
                order.price = order_info["price"]
        else:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["size"] > 0 else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": float(order_info["price"]),
                "quantity": abs(quantity),
                "trade_type": int(order_info["text"].split('-')[1]),
                "remain": abs(remain),
                "status": status,
                "avg_price": order_info["fill_price"]
            }
            order = Order(**info)
            self._orders[order_no] = order
            is_updated = True
        order.ctime = ctime
        order.utime = int(time.time())
        if status in [ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)
        return is_updated, order




