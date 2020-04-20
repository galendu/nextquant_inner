# encoding: utf-8
"""
Fcoin 合约 REST API 封装
https://developer.fcoin.com/zh.html

Author: April
Date:   2019/07/20
"""
import base64
import copy
import hashlib
import hmac
import json
from urllib.parse import urljoin, quote

import time

import datetime

from quant.asset import AssetSubscribe, Asset
from quant.const import FCOIN_FUTURE
from quant.error import Error
from quant.order import ORDER_TYPE_LIMIT, ORDER_ACTION_BUY, ORDER_STATUS_FAILED, ORDER_STATUS_PARTIAL_FILLED, \
    ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_CANCELED, ORDER_STATUS_CANCELED, ORDER_ACTION_SELL, TRADE_TYPE_NONE, \
    Order
from quant.position import Position
from quant.tasks import SingleTask, LoopRunTask
from quant.utils import logger
from quant.utils.decorator import async_method_locker
from quant.utils.http_client import AsyncHttpRequests

__all__ = ("FCoinFutureRestAPI", "FCoinFutureTrade")


class FCoinFutureRestAPI:
    """
    Fcoin 合约 REST API封装
    """

    def __init__(self, host, access_key, secret_key):
        """ 初始化
        @param host 请求的host
        @param access_key 请求的access_key
        @param secret_key 请求的secret_key
        """
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_future_accounts(self):
        """
        获取合约账户资产列表
        """
        uri = 'v3/contracts/accounts'
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_position_list(self):
        uri = 'v3/contracts/positions'
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_index(self):
        """获取当前系统指数"""
        uri = 'v2/market/indexes'
        success, error = await self.request('GET', uri)
        return success, error

    async def create_order(self, instrument_id, trade_type, price, quantity, order_type=ORDER_TYPE_LIMIT):
        body = {
            'symbol': instrument_id,
            'type': 'LIMIT' if order_type == ORDER_TYPE_LIMIT else 'MARKET',
            'quantity': abs(quantity),
            # 'post_only': True
        }

        if order_type == ORDER_TYPE_LIMIT:
            body['price'] = price

        if trade_type == 1 or trade_type == 4:  # 开多/平空(买入)
            body['direction'] = 'LONG'
        else:
            body['direction'] = 'SHORT'

        body['source'] = str(trade_type)

        uri = '/v3/contracts/orders'

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_order_info(self, order_id):
        """查询open订单"""
        uri = 'v3/contracts/orders/open/{}'.format(str(order_id))
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_closed_order_list(self, symbol):
        """查询已关闭的(fully_cancelled/fully_filled)订单列表"""
        uri = '/v3/contracts/orders/closed'
        params = {
            'symbol': symbol,
            'limit': 10
        }
        success, error = await self.request('GET', uri, params=params, auth=True)
        return success, error

    async def get_open_order_list(self):
        """查询未关闭的订单"""
        uri = '/v3/contracts/orders/open'
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_order_deal_history(self, order_id):
        """查询订单成交历史"""
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        range = str(year) + '%02d' % month
        uri = 'v3/contracts/orders/{}/matches'.format(order_id)
        params = {
            'range': range
        }
        success, error = await self.request('GET', uri, params=params, auth=True)
        return success, error

    async def revoke_order(self, order_id):
        uri = 'v3/contracts/orders/{}/cancel'.format(order_id)
        success, error = await self.request('POST', uri, auth=True)
        # status=1797代表order找不到
        return success, error

    async def request(self, method, uri, params=None, body=None, auth=False):
        """ 发起请求
        @param method 请求方法 GET / POST / DELETE / PUT
        @param uri 请求uri
        @param params dict 请求query参数
        @param body dict 请求body数据
        @param headers 请求http头
        @param auth boolean 是否需要加入权限校验
        @:return: 请求成功success为返回数据，error为None，请求失败，success为None，error为报错信息
        """
        # query = ''
        if params:
            # 查询参数需要按照按照字母表排序
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += '?' + query

        post_body_str = ''
        if body:
            post_body_str = "&".join(["{}={}".format(k, body[k]) for k in sorted(body.keys())])
            body = json.dumps(body)

        url = urljoin(self._host, uri)

        headers = {
            'Content-Type': 'application/json'
        }

        # 增加签名
        if auth:
            timestamp = str(int(time.time() * 1000))
            if method == 'GET':
                sig_str = method + url + timestamp
            else:
                sig_str = method + url + timestamp + post_body_str
            # print(sig_str)

            # 先进行base64编码
            sign_step1 = base64.b64encode(bytes(sig_str, 'utf-8'))
            # 对编码后的数据进行 HMAC-SHA1 签名，并对签名进行二次 Base64 编码
            signature = base64.b64encode(
                hmac.new(bytes(self._secret_key, 'utf-8'), sign_step1, digestmod=hashlib.sha1).digest())

            headers['FC-ACCESS-KEY'] = self._access_key
            headers['FC-ACCESS-SIGNATURE'] = signature.decode()
            headers['FC-ACCESS-TIMESTAMP'] = timestamp

        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        print(_, success, error)
        return success, error


class FCoinFutureTrade:
    """ FCOIN Future Trade module
        """

    def __init__(self, **kwargs):
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.testnet.fmex.com/"
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
        self._platform = FCOIN_FUTURE
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._update_order_interval = kwargs.get("update_order_interval", 1)

        self._assets = {}  # 资产 {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # 订单
        self._position = Position(self._platform, self._account, self._strategy, self._symbol)  # 仓位

        # 标记订阅订单、持仓是否成功
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False

        # 初始化 REST API 对象
        self._rest_api = FCoinFutureRestAPI(self._host, self._access_key, self._secret_key)

        # 初始化资产订阅
        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        # 注册定时任务，定时查询订单更新、仓位更新情况
        if self._position_update_callback:
            LoopRunTask.register(self._check_position_update, 60)

        # 初始化订单订阅
        if self._order_update_callback:
            LoopRunTask.register(self.check_order_update, self._update_order_interval)

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
    def position(self):
        return copy.copy(self._position)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ 创建订单
        @param action 交易方向 BUY/SELL
        @param price 委托价格
        @param quantity 委托数量(可以是正数，也可以是复数)
        @param order_type 委托类型 limit/market
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
        quantity = abs(int(quantity))
        result, error = await self._rest_api.create_order(self._symbol, trade_type, price, quantity)
        if error:
            return None, error
        return result['data']["id"], None

    async def revoke_order(self, *order_nos):
        """ 撤销订单
            @param order_nos 订单号，可传入任意多个，如果不传入，那么就撤销所有订单
            * NOTE: 单次调用最多只能撤销100个订单，如果订单超过100个，请多次调用
        """
        # 如果传入order_nos为空，即撤销全部委托单
        if len(order_nos) == 0:
            result, error = self._rest_api.get_open_order_list()
            if error:
                return False, error
            success, error = [], []
            for order_info in result['data']['results']:
                order_no = order_info['id']
                _, e = await self._rest_api.revoke_order(order_no)
                if e:
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return success, error

        # 如果传入order_nos为一个委托单号，那么只撤销一个委托单
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # 如果传入order_nos数量大于1，那么就批量撤销传入的委托单
        if len(order_nos) > 1:
            success, error = [], []
            for order_no in order_nos:
                _, e = await self._rest_api.revoke_order(order_no)
                if e:
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return success, error

    async def get_open_order_info(self, order_no):
        """查询活动订单详情"""
        result, error = await self._rest_api.get_open_order_info(order_no)

        if error:
            return None, error
        else:
            for data in result['data']['results']:
                if data['id'] == order_no:
                    logger.info(data)
                    is_update, order = self._update_order(data)
                    if is_update and self._order_update_callback:
                        SingleTask.run(self._order_update_callback, copy.copy(order))
                    return copy.copy(order), None

    async def get_open_order_nos(self):
        """ 获取未完全成交订单号列表
        """
        success, error = await self._rest_api.get_open_order_list()
        if error:
            return None, error
        else:
            order_nos = []
            for order_info in success['data']['results']:
                order_nos.append(order_info['id'])
            return order_nos, None

    @async_method_locker('check_order_update', False)
    async def check_order_update(self):
        """定时查询订单信息"""
        open_result, error = await self._rest_api.get_open_order_list()
        closed_result, error = await self._rest_api.get_closed_order_list(self._symbol)

        orders = list()
        # 查询所有未完成订单
        if open_result:
            open_orders = open_result['data']['results']
            orders += open_orders

        # 查询所有已关闭订单
        if closed_result:
            close_orders = open_result['data']['results']
            orders += close_orders

        if len(orders) != 0:
            for order_info in copy.copy(orders):
                is_update, order = await self._update_order(order_info)
                if is_update:
                    if self._order_update_callback:
                        SingleTask.run(self._order_update_callback, copy.copy(order))

    async def _update_order(self, order_info):
        """
        更新订单信息
        """
        order_no = order_info.get('id')
        is_updated = False
        if order_no is None:
            return is_updated, None
        remain = order_info['unfilled_quantity']
        quantity = order_info['quantity']
        state = order_info['status']
        ctime = order_info['created_at']
        utime = order_info['utime']
        if state == 'FULLY_FILLED':
            status = ORDER_STATUS_FAILED
        elif state == 'PARTIAL_FILLED':
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == 'PENDING':
            status = ORDER_STATUS_SUBMITTED
        elif state == 'PARTIAL_CANCELLED':
            status = ORDER_STATUS_PARTIAL_CANCELED
        elif state == 'FULLY_CANCELLED':
            status = ORDER_STATUS_CANCELED
        else:
            logger.error('订单状态未处理', order_info)
            return is_updated, None

        order = self._orders.get(order_no)
        if not order:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["direction"] == 'LONG' else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": quantity,
                "trade_type": TRADE_TYPE_NONE,
                'remain': remain,
                'status': status
            }
            order = Order(**info)
            is_updated = True
        else:
            if order.remain != remain:
                is_updated = True
                order.remain = remain
            elif order.status != status:
                is_updated = True
                order.status = status
        order.ctime = ctime
        order.utime = utime
        self._orders[order_no] = order
        if state in ('FULLY_FILLED', 'PARTIAL_CANCELLED', 'FULLY_CANCELLED'):
            self._orders.pop(order_no)
        return is_updated, order

    async def _check_position_update(self):
        """更新持仓信息"""
        positions, error = await self._rest_api.get_position_list()
        if error:
            logger.error("get position error: {}".format(error))
            return

        for position_info in positions['data']['results']:
            if position_info['symbol'] == self._symbol:
                update = False
                if not self._position.utime:  # 如果持仓还没有被初始化，那么初始化之后推送一次
                    update = True
                    self._position.update()
                size = position_info['quantity']
                average_price = position_info['entry_price']
                liquid_price = position_info['liquidation_price']
                direction = position_info['direction']
                if direction == 'SHORT':
                    if self._position.short_quantity != size:
                        update = True
                        self._position.update(size, average_price, 0, 0, liquid_price)
                else:
                    if self._position.long_quantity != size:
                        update = True
                        self._position.update(0, 0, size, average_price, liquid_price)
                if update and self._position_update_callback:
                    SingleTask.run(self._position_update_callback, copy.copy(self._position))
