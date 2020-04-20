# encoding: utf-8
"""
Fcoin 币币杠杆 REST API 封装
https://developer.fcoin.com/zh.html

Author: April
Date:   2019/07/20
"""

import time
import json
import hmac
import copy
import zlib
import base64
from urllib.parse import urljoin
import hashlib
import asyncio

from quant.error import Error
from quant.utils import tools
from quant.utils import logger
from quant.const import FCOIN
from quant.order import Order
from quant.tasks import SingleTask, LoopRunTask
from quant.utils.websocket import Websocket
from quant.asset import Asset, AssetSubscribe
from quant.utils.decorator import async_method_locker
from quant.utils.http_client import AsyncHttpRequests
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED, ORDER_STATUS_PENDING_CANCEL, ORDER_STATUS_PARTIAL_CANCELED

__all__ = ("FCoinRestAPI", "FCoinMarginTrade")


class FCoinRestAPI:
    """
    FCoin 币币杠杆 REST APT封装
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

    async def get_margin_accounts(self):
        """
        获取币币杠杆账户资产列表
        """
        uri = 'broker/leveraged_accounts'
        success, error = await self.request('GET', uri, auth=True)
        return success, error

    async def get_margin_account(self, instrument_id):
        """
         单一币对账户信息，币对示例：btcusdt
        """
        uri = 'accounts/balance'
        success, error = await self.request('GET', uri, params={'account_type': instrument_id}, auth=True)
        return success, error

    async def create_order(self, action, instrument_id, price, quantity, order_type=ORDER_TYPE_LIMIT, exchange='main'):
        """ 下单
        @param action 操作类型 BUY SELL
        @param instrument_id 币对名称
        @param price 价格(市价单传入None)   字符串
        @param quantity 买入或卖出的数量 限价买入或卖出数量 / 市价买入金额 / 市价卖出数量    字符串
        @param order_type 交易类型，市价 / 限价
        @param account_type 账户类型，(币币交易不需要填写，杠杆交易：margin)
        """
        body = {
            'symbol': instrument_id,
            'side': "buy" if action == ORDER_ACTION_BUY else "sell",
            'type': 'limit' if order_type == ORDER_TYPE_LIMIT else 'market',
            'amount': quantity,
            'exchange': exchange
        }

        if order_type == ORDER_TYPE_LIMIT:
            body['price'] = price

        uri = 'orders'
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, order_id):
        """
        撤单
        :param order_id: 订单ID
        """
        uri = 'orders/{order_id}/submit-cancel'.format(order_id=order_id)
        success, error = await self.request("POST", uri, auth=True)
        return success, error

    async def get_order_list(self, instrument_id, status, before=None, after=None, account_type=None, limit=20):
        """
        获取订单列表
        :param instrument_id:  交易对
        :param status: 订单状态,只能查询单种状态，submitted,partial_filled,partial_canceled,filled,canceled
        :param before:  查询某个时间戳之前的订单
        :param after:  查询某个时间戳之后的订单
        :param account_type:   杠杆账户：margin
        :param limit:  每页的订单数量，默认为 20 条，最大100
        """
        params = {
            'symbol': instrument_id,
            'states': status,
            'limit': limit,
        }
        # before和after最多只能携带其中一个
        if before:
            params['before'] = before

        if after:
            params['after'] = after

        if account_type:
            params['account_type'] = account_type
        uri = 'orders'
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_open_orders(self, instrument_id, account_type=None):
        success1, error1 = await self.get_order_list(instrument_id, 'submitted', account_type=None)
        if error1:
            return success1, error1
        success2, error2 = await self.get_order_list(instrument_id, 'partial_filled', account_type=None)
        if error2:
            return success2, error2
        success1['data'].extend(success2['data'])
        return success1, error1

    async def get_order(self, order_id):
        """
        查询订单详情
        :param order_id: 订单ID
        """
        uri = 'orders/{order_id}'.format(order_id=order_id)
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def get_candle(self, resolution, instrument_id, before=None, limit=20):
        """
        # 获取k线
        :param resolution: k线频率
        M1	1分钟、M3	3分钟、M5	5分钟、M15	15分钟、M30  30分钟、
        H1	1小时、H4	4小时、H6	6小时、D1	1日、W1	  1周、MN	 1月
        :param instrument_id:
        :param before:  k线id，时间戳，单位为s，查询某个id之前的 Candle
        :param limit: 默认为 20 条
        """
        uri = 'market/candles/{resolution}/{symbol}'.format(resolution=resolution, symbol=symbol)
        params = {
            'limit': limit
        }
        if before:
            params['before'] = before
        success, error = await self.request("GET", uri, params=params)
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
        if params:
            # 查询参数需要按照按照字母表排序
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += '?' + query
        url = urljoin(self._host, uri)

        post_body_str = ''
        if body:
            post_body_str = "&".join(["{}={}".format(k, body[k]) for k in sorted(body.keys())])
            body = json.dumps(body)

        headers = {
            'Content-Type': 'application/json'
        }

        # 增加签名
        if auth:
            timestamp = str(int(time.time() * 1000))
            if method == 'GET':
                sig_str = method + url + timestamp
            elif method == 'POST':
                sig_str = method + url + timestamp + post_body_str

            # 先进行base64编码
            sign_step1 = base64.b64encode(bytes(sig_str, 'utf-8'))
            # 对编码后的数据进行 HMAC-SHA1 签名，并对签名进行二次 Base64 编码
            signature = base64.b64encode(
                hmac.new(bytes(self._secret_key, 'utf-8'), sign_step1, digestmod=hashlib.sha1).digest())

            headers['FC-ACCESS-KEY'] = self._access_key
            headers['FC-ACCESS-SIGNATURE'] = signature.decode()
            headers['FC-ACCESS-TIMESTAMP'] = timestamp

        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        return success, error


class FCoinMarginTrade:
    """ FCoin Margin Trade模块
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
            kwargs["host"] = 'https://api.fcoin.com/v2/'
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
        self._platform = FCOIN
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._update_order_interval = kwargs.get("update_order_interval", 1)

        self._raw_symbol = self._symbol.replace("/", "").lower()  # 转换成交易所对应的交易对格式

        self.heartbeat_msg = 'ping'

        self._assets = {}  # 资产 {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # 订单 {"order_no": order, ... }

        # 初始化 REST API 对象
        self._rest_api = FCoinRestAPI(self._host, self._access_key, self._secret_key)

        # 初始化资产订阅
        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        # 初始化订单订阅
        # if self._order_update_callback:
        # # 注册定时任务，定时获取订单
        #     LoopRunTask.register(self.get_order_info, self._update_order_interval)

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, exchange='main'):
        """ 创建订单
        @param action 交易方向 BUY/SELL
        @param price 委托价格
        @param quantity 委托数量
        @param order_type 委托类型 LIMIT / MARKET
        """
        if order_type == ORDER_TYPE_LIMIT and price:
            price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)

        result, error = await self._rest_api.create_order(action, self._raw_symbol, price, quantity, order_type, exchange)
        if error:
            return None, error
        if result['status'] != 0:
            return None, result
        order_id = result["data"]

        # 创建订单成功后，将订单存下来
        info = {
            "platform": self._platform,
            "account": self._account,
            "strategy": self._strategy,
            "order_no": order_id,
            "action": action,
            "symbol": self._symbol,
            "price": price,  # 字符串
            "quantity": quantity,
            "remain": quantity,
            "status": ORDER_STATUS_SUBMITTED,
            "avg_price": price,
            'order_type': order_type
        }
        order = Order(**info)
        self._orders[order_id] = order

        return order_id, None

    async def revoke_order(self, *order_nos):
        """ 撤销订单
        @param order_nos 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单
        * NOTE: 单次调用最多只能撤销4个订单，如果订单超过4个，请多次调用
        """
        # 如果传入order_nos为空，即撤销全部委托单
        if len(order_nos) == 0:
            order_nos = self._orders.keys()
            success, error = [], []
            for order_no in order_nos:
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

    async def get_open_order_nos(self):
        """ 获取未完全成交订单号列表
        """
        success, error = await self._rest_api.get_open_orders(self._raw_symbol, account_type='margin')
        if error:
            return None, error
        else:
            order_nos = []
            for order_info in success['data']:
                order_nos.append(order_info["id"])
            return order_nos, None

    @async_method_locker('get_order_info', False)
    async def get_order_info(self):
        """定时查询未完成订单的信息"""
        order_ids = self._orders.keys()
        if len(order_ids) == 0:
            return
        for id in order_ids:
            s, e = self._rest_api.get_order(id)
            if not e:
                is_updated, order = self._update_order(s['data'])
                if self._order_update_callback and is_updated:
                    SingleTask.run(self._order_update_callback, copy.copy(order))
            asyncio.sleep(0.2)

    def _update_order(self, order_info):
        """ 更新订单信息
        @param order_info 订单信息
        """
        is_updated = False  # 当前订单相比之前是否有更新
        order_no = order_info['id']
        state = order_info["state"]
        remain = float(order_info["amount"]) - float(order_info["filled_amount"])
        ctime = order_info["created_at"]

        if state == "canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "submitted":
            status = ORDER_STATUS_SUBMITTED
        elif state == "partial_filled":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "filled":
            status = ORDER_STATUS_FILLED
        elif state == 'partial_canceled':
            status = ORDER_STATUS_PARTIAL_CANCELED
        elif state == 'pending_cancel':
            status = ORDER_STATUS_PENDING_CANCEL
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
            elif order.price != order_info["price"]:
                is_updated = True
                order.price = order_info["price"]
        else:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": order_info["amount"],
                "remain": remain,
                "status": status,
                "avg_price": order_info["price"]
            }
            order = Order(**info)
            self._orders[order_no] = order
            is_updated = True
        order.ctime = ctime
        if status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED, ORDER_STATUS_PARTIAL_CANCELED]:
            self._orders.pop(order_no)
        return is_updated, order

    async def on_event_asset_update(self, asset: Asset):
        """ 资产数据更新回调
        """
        self._assets = asset
        SingleTask.run(self._asset_update_callback, asset)
