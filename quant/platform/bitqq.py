# encoding: utf-8
import json
import time
import hmac
import copy
import binascii
import asyncio
from urllib.parse import urljoin, quote
import hashlib

from quant import const
from quant.error import Error
from quant.utils import logger
from quant.const import BITQQ
from quant.order import Order
from quant.tasks import SingleTask, LoopRunTask
from quant.asset import Asset, AssetSubscribe
from quant.utils.http_client import AsyncHttpRequests
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED, ORDER_STATUS_PENDING_CANCEL
from base64 import b64encode
from Crypto.Cipher import AES

__all__ = ("BitQQRestAPI", "BitQQTrade")


def convert_md5(origin):
    result = []
    s = ""
    for i in range(len(origin)):
        s += origin[i]
        if i % 2 != 0:
            int_hex = int(s, 16)
            result.append(int_hex)
            s = ""

    return result


def encryption_md5_buy_key(data):
    key = 'T5xJUNDA6hzxBuuwx8arhsDxCNGbO7iL'
    encode_data1 = data.encode()
    result1 = hmac.new(key.encode(), encode_data1, digestmod='MD5').hexdigest()
    last_result = convert_md5(result1)
    l_result = bytearray(last_result)
    lll_result = b64encode(l_result)
    return lll_result.decode()


class AESCipher:
    """AES ECB 128位加密"""

    def __init__(self, key, BLOCK_SIZE):
        self.key = key
        self.BLOCK_SIZE = BLOCK_SIZE

    def pad(self, s):
        return s + (self.BLOCK_SIZE - len(s) % self.BLOCK_SIZE) * chr(self.BLOCK_SIZE - len(s) % self.BLOCK_SIZE)

    def unpad(self, s):
        return s[:-ord(s[len(s) - 1:])]

    def encrypt(self, raw):
        raw = self.pad(raw)
        cipher = AES.new(self.key, AES.MODE_ECB)
        ret = cipher.encrypt(raw.encode())  # 加密pwd原文得到秘文pwd
        return ret.hex()  # 将秘文转换为16进制

    def decrypt(self, enc):
        enc = binascii.unhexlify(enc)
        cipher = AES.new(self.key, AES.MODE_ECB)
        return self.unpad(cipher.decrypt(enc)).decode('utf8')


class BitQQRestAPI:

    def __init__(self, host, access_key, secret_key, passphrase, order_module_host=None):
        """因暂时未开放api，则access代表账号， secret代表密码"""
        self._host = host
        self._order_module_host = order_module_host
        self._access_key = access_key
        self.secret_key = secret_key
        self.passphrase = passphrase  # 账号id
        # self.token = None
        # SingleTask.run(self.login)
        # LoopRunTask.register(self.login, interval=60 * 60)

    async def get_spot_accounts(self):
        uri = 'api/userfund/list'
        success, error = await self.request('GET', uri, auth=True)
        # print(success, error)
        return success, error

    # async def login(self, *args, **kwargs):
    #     t = int(time.time() * 1000)
    #     key = "mN4Yn8Or8r7SH1w4VnpS5lMS"
    #     BLOCK_SIZE = 16  # Bytes
    #     md = hashlib.md5()
    #     md.update(key.encode())
    #
    #     ret = md.hexdigest()  # 将密钥进行md5加密并获取加密后的密文
    #     ret_16bit = convert_md5(ret)  # 将密文转换为16位的数组
    #     bytess = bytearray(ret_16bit)  # 将数组转换为bytearray
    #
    #     aes = AESCipher(bytess, BLOCK_SIZE)
    #     en = aes.encrypt(self.secret_key)  # 加密pwd
    #     ss = self.secret_key + str(t) + en
    #     sign = encryption_md5_buy_key(ss)  # 生成签名
    #
    #     uri = 'api/user/login'
    #
    #     sign = sign.replace('+', '%2B')
    #
    #     params = {
    #         'userAccount': self._access_key,
    #         'password': en,
    #         'device': 'oNrTBq4u3gP9G0ns2SoKypG9X',
    #         'loginTime': t,
    #         'sign': sign,
    #         'type': 2
    #     }
    #     while True:
    #         success, error = await self.request('POST', uri, params=params, auth=False)
    #         if success:
    #             if success['state'] == 0:
    #                 self.token = success['token']
    #                 logger.info('登录成功', self.token)
    #             else:
    #                 self.token = None
    #         else:
    #             self.token = None
    #             logger.error('登录失败', error)
    #         if self.token:
    #             break
    #         asyncio.sleep(2)

    async def create_order(self, action, symbol, price, quantity, *args):
        """

        :param action:
        :param symbol: 交易对符号 例 eos_usdt
        :param price: 价格，float
        :param quantity: 交易量 float
        :param order_type:
        :param account_type:  1、币币 2、杠杆
        :return:
        """
        uri = 'coin/entrust/robot/order'
        is_order_module = False
        if self._order_module_host:
            uri = 'entrust/robot/order'
            is_order_module = True
        params = {
            'type': "buy" if action == ORDER_ACTION_BUY else "sell",
            'amount': quantity,
            'price': price,
            'symbol': symbol,
        }

        success, error = await self.request('POST', uri=uri, params=params, auth=True, is_order_module=is_order_module)

        return success, error

    async def revoke_order(self, order_no):
        uri = 'coin/entrust/revoke'
        is_order_module = False
        if self._order_module_host:
            uri = 'entrust/revoke'
            is_order_module = True

        params = {
            'orderId': order_no
        }
        success, error = await self.request('POST', uri=uri, params=params, auth=True, is_order_module=is_order_module)

        return success, error

    async def revoke_orders(self, *order_ids):
        uri = 'coin/entrust/robot/batchOrder'
        is_order_module = False
        if self._order_module_host:
            uri = 'entrust/robot/batchOrder'
            is_order_module = True

        order_ids_str = [str(id) for id in order_ids]
        if len(order_ids_str) > 12:
            order_ids_str = order_ids_str[:12]
        params = {
            'orderIds': ','.join(order_ids_str)
        }

        success, error = await self.request('POST', uri=uri, params=params, auth=True, is_order_module=is_order_module)

        return success, error

    async def get_user_account(self):
        url = 'api/userfund/list'
        success, error = await self.request('GET', uri=url, auth=True)
        return success, error

    async def get_order_info(self, order_no):
        uri = 'coin/user/robot/orderDetail'
        is_order_module = False
        if self._order_module_host:
            uri = 'user/robot/orderDetail'
            is_order_module = True
        params = {
            'orderId': order_no
        }
        success, error = await self.request('POST', uri=uri, params=params, auth=True, is_order_module=is_order_module)

        return success, error

    async def get_order_list(self, symbol):
        uri = 'coin/user/robot/currOrderList'
        is_order_module = False
        if self._order_module_host:
            uri = 'user/robot/currOrderList'
            is_order_module = True
        params = {
            'symbol': symbol,
            'module': 1,
            'page': 0,
            'limit': 999
        }
        success, error = await self.request('POST', uri=uri, params=params, auth=True, is_order_module=is_order_module)
        return success, error

    async def get_kline(self, symbol, kline_type=const.MARKET_TYPE_KLINE, start=None, limit=20):
        uri = 'public/market/kline'
        params = {
            'symbol': symbol,
            'page': 1,
            'limit': limit
        }
        if kline_type == const.MARKET_TYPE_KLINE:
            params['timeType'] = 1
        elif kline_type == const.MARKET_TYPE_KLINE_5M:
            params['timeType'] = 2
        elif kline_type == const.MARKET_TYPE_KLINE_15M:
            params['timeType'] = 3
        elif kline_type == const.MARKET_TYPE_KLINE_30M:
            params['timeType'] = 4
        elif kline_type == const.MARKET_TYPE_KLINE_1H:
            params['timeType'] = 5
        elif kline_type == const.MARKET_TYPE_KLINE_24H:
            params['timeType'] = 6

        success, error = await self.request('POST', uri=uri, params=params)
        if success:
            kline_list = list()
            klines = success['data']['list']
            for kline in klines:
                ts_str = kline['createDate']
                ts = int(time.mktime(time.strptime(ts_str, '%Y-%m-%d %H:%M:%S'))) * 1000
                kline['createDate'] = ts
                if ts >= start:
                    kline_list.append(kline)
            if len(kline_list) > limit:
                return kline_list[0:limit], None
            else:
                return kline_list
        else:
            return success, error

    async def get_latest_price(self, symbol):
        """获取最新成交价"""
        uri = '/public/market/lastOrder'
        params = {
            'symbol': symbol,
            'limit': 1
        }

        success, error = await self.request('POST', uri=uri, params=params)
        return success, error

    async def request(self, method, uri, params=None, body=None, auth=False, is_order_module=False):
        """ 发起请求
        @param method 请求方法 GET / POST / DELETE / PUT
        @param uri 请求uri
        @param params dict 请求query参数
        @param body dict 请求body数据
        @param headers 请求http头
        @param auth boolean 是否需要加入权限校验
        @:return: 请求成功success为返回数据，error为None，请求失败，success为None，error为报错信息
        """
        # 增加签名
        if auth:
            if params is None:
                params = dict()

            params['userId'] = self.passphrase

        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += '?' + query
        url = urljoin(self._host, uri)
        if self._order_module_host and is_order_module:
            url = urljoin(self._order_module_host, uri)

        headers = {
            'Content-Type': 'application/json'
        }
        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=30)
        # print('原始结果:', success, error)
        logger.debug(url)
        if success:
            try:
                if isinstance(success, str):
                    success = json.loads(success)
                if success.get('status') != 0 or success.get('msg') != 'success':
                    return None, success
            except Exception as e:
                return None, e

        return success, error


class BitQQTrade:

    def __init__(self, **kwargs):
        """
        初始化
        """
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("host"):
            kwargs["host"] = "http://dev.api.bitqq.vip:81"
        if not kwargs.get("wss"):
            kwargs['wss'] = 'wss://dev.websocket.bitqq.vip:9094'
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("passphrase"):
            e = Error("param passphrase miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = BITQQ
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._order_module_host = kwargs.get('order_module_host')
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._contract_update_callback = kwargs.get('contract_update_callback')

        # 初始化 REST API 对象
        self._rest_api = BitQQRestAPI(self._host, self._access_key, self._secret_key, self._passphrase, order_module_host=self._order_module_host)

        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        SingleTask.call_later(self.reset_order_list, delay=10)

    @property
    def rest_api(self):
        return self._rest_api

    async def on_event_asset_update(self, asset: Asset):
        """资产数据更新回调"""
        self._assets = asset
        SingleTask.run(self._asset_update_callback, asset)

    async def reset_order_list(self):
        # 撤销当前账号所有
        success, error = await self.revoke_all_order()
        if error:
            logger.error('撤销所有订单失败, error: ', error)
            SingleTask.call_later(self.reset_order_list, delay=1)
        else:
            if self._init_success_callback:
                SingleTask.run(self._init_success_callback, True, None)

    async def get_latest_price(self, symbol=None):
        if symbol is None:
            symbol = self._symbol
        success, error = await self._rest_api.get_latest_price(symbol)
        if success:
            if len(success['data']) > 0:
                success = success['data'][0]['price']
            else:
                success = None
        return success, error

    async def revoke_order(self, *order_nos):
        if len(order_nos) == 0:
            return [], Error('订单号传参错误')
        else:
            # 批量撤单
            result, error = await self._rest_api.revoke_orders(*order_nos)
            if error:
                for id in order_nos:
                    await self.get_order_info(id)
                    await asyncio.sleep(0.05)
                return [], error
            else:
                revoked_orders = result.get('data', [])
                # logger.info('批量撤单结果：', revoked_orders)
                for id in order_nos:
                    if id not in revoked_orders:
                        await self.get_order_info(id)
                        await asyncio.sleep(0.05)
                return revoked_orders, None

    async def create_order(self, action, price, quantity, *args):
        result, error = await self._rest_api.create_order(action, self._symbol, price, quantity)
        if error:
            return None, error
        else:
            return result.get('data'), None

    def _update_order(self, order_info=None, s_order_id=None):
        """ 更新订单信息
        """
        order = None
        if order_info:
            logger.debug('查询订单信息, order: ', order_info)
            order_no = str(order_info['id'])
            state = str(order_info["status"])
            remain = float(order_info["surplusCount"])
            utime = order_info["updateTime"]
            ctime = order_info["createDate"]
            action = ORDER_ACTION_BUY if str(order_info['orderType']) == "1" else ORDER_ACTION_SELL

            if state == "5":
                status = ORDER_STATUS_CANCELED
            elif state == "2":
                status = ORDER_STATUS_PARTIAL_FILLED
            elif state == "3":
                status = ORDER_STATUS_FILLED
            elif state == "4":
                status = ORDER_STATUS_PENDING_CANCEL
            else:
                status = ORDER_STATUS_SUBMITTED

            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": action,
                "symbol": order_info['symbol'],
                "price": order_info["putPrice"],
                "quantity": order_info["count"],
            }
            order = Order(**info)

            if order_info.get('dealPrice') is None:
                avg_price = 0.0
            else:
                avg_price = float(order_info.get('dealPrice'))

            order.remain = remain
            order.status = status
            order.avg_price = avg_price
            order.ctime = ctime
            order.utime = utime

        if s_order_id:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": str(s_order_id),
                "symbol": self._symbol,
            }
            order = Order(**info)
            order.status = ORDER_STATUS_CANCELED

        if order and self._order_update_callback:
            SingleTask.run(self._order_update_callback, copy.copy(order))

        return order

    async def revoke_all_order(self):

        data, error = await self._rest_api.get_order_list(self._symbol)
        if error:
            return None, error

        else:
            open_order_ids = [item['id'] for item in data['data']['list']]
            if len(open_order_ids) == 0:
                return [], None
            result, error = await self._rest_api.revoke_orders(*open_order_ids)
            if error:
                    return None, error
            else:
                revoked_order_ids = result.get('data', [])
                if revoked_order_ids == open_order_ids:
                    return [], None
                else:
                    unrevoked_num = len(open_order_ids) - len(revoked_order_ids)
                    if unrevoked_num > 0:
                        unrevoked_orders = [id for id in open_order_ids if id not in revoked_order_ids]
                        return None, Error('还剩余{}个订单等待撤销，订单号为 {}'.format(unrevoked_num, unrevoked_orders))
                    else:
                        return [], None

    async def get_order_info(self, order_no):
        order_info, error = await self._rest_api.get_order_info(order_no)

        if error:
            if error:
                logger.debug('查询订单详情失败，订单id: {}, error:{}'.format(order_no, error))
                if isinstance(error, dict) and error.get('state') == 2500:
                    logger.debug('订单已被数据库删除, id:', order_no)
                    order = self._update_order(s_order_id=order_no)
                    return copy.copy(order), None
            return None, error
        else:
            order = self._update_order(order_info['data'])
            return copy.copy(order), None
