# -*— coding:utf-8 -*-

"""
Event Center.

Author: HuangTao
Date:   2018/05/04
Email:  huangtao@ifclover.com
"""

import json
import zlib
import asyncio

import aioamqp

from quant import const
from quant.position import PositionObject
from quant.pre_warning import PreWarning
from quant.utils import logger
from quant.config import config
from quant.tasks import LoopRunTask, SingleTask
from quant.utils.decorator import async_method_locker
from quant.market import Orderbook, Trade, Kline
from quant.asset import Asset
from quant.order import Order
from quant.position import Position
from quant.handle import EditVar


__all__ = ("EventCenter", "EventConfig", "EventHeartbeat", "EventAsset", "EventOrder", "EventKline", "EventOrderbook",
           "EventTrade", "EventPosition", "EventPreWarning")


class Event:
    """ Event base.

    Attributes:
        name: Event name.
        exchange: Exchange name.
        queue: Queue name.
        routing_key: Routing key name.
        pre_fetch_count: How may message per fetched, default is 1.
        data: Message content.
    """

    def __init__(self, name=None, exchange=None, queue=None, routing_key=None, pre_fetch_count=1, data=None):
        """Initialize."""
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._pre_fetch_count = pre_fetch_count
        self._data = data
        self._callback = None  # Asynchronous callback function.

    @property
    def name(self):
        return self._name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def prefetch_count(self):
        return self._pre_fetch_count

    @property
    def data(self):
        return self._data

    def dumps(self):
        d = {
            "n": self.name,
            "d": self.data
        }
        s = json.dumps(d)
        b = zlib.compress(s.encode("utf8"))
        return b

    def loads(self, b):
        b = zlib.decompress(b)
        d = json.loads(b.decode("utf8"))
        self._name = d.get("n")
        self._data = d.get("d")
        return d

    def parse(self):
        raise NotImplemented

    def subscribe(self, callback, multi=False):
        """ Subscribe this event.

        Args:
            callback: Asynchronous callback function.
            multi: If subscribe multiple channels ?
        """
        from quant.quant import quant
        self._callback = callback
        SingleTask.run(quant.event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """Publish this event."""
        from quant.quant import quant
        SingleTask.run(quant.event_center.publish, self)

    async def callback(self, channel, body, envelope, properties):
        self._exchange = envelope.exchange_name
        self._routing_key = envelope.routing_key
        self.loads(body)
        o = self.parse()
        await self._callback(o)

    def __str__(self):
        info = "EVENT: name={n}, exchange={e}, queue={q}, routing_key={r}, data={d}".format(
            e=self.exchange, q=self.queue, r=self.routing_key, n=self.name, d=self.data)
        return info

    def __repr__(self):
        return str(self)


class EventConfig(Event):
    """ Config event.

    Attributes:
        server_id: Server id.
        params: Config params.

    * NOTE:
        Publisher: Manager Server.
        Subscriber: Any Servers who need.
    """

    def __init__(self, server_id=None, params=None):
        """Initialize."""
        name = "EVENT_CONFIG"
        exchange = "Config"
        queue = "{server_id}.{exchange}".format(server_id=server_id, exchange=exchange)
        routing_key = "{server_id}".format(server_id=server_id)
        data = {
            "server_id": server_id,
            "params": params
        }
        super(EventConfig, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        return self._data


class EventHeartbeat(Event):
    """ Server Heartbeat event.

    Attributes:
        server_id: Server id.
        count: Server heartbeat count.

    * NOTE:
        Publisher: All servers
        Subscriber: Monitor server.
    """

    def __init__(self, server_id=None, count=None):
        """Initialize."""
        name = "EVENT_HEARTBEAT"
        exchange = "Heartbeat"
        queue = "{server_id}.{exchange}".format(server_id=server_id, exchange=exchange)
        routing_key = "{server_id}".format(server_id=server_id)
        data = {
            "server_id": server_id,
            "count": count
        }
        super(EventHeartbeat, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        return self._data


class EventAsset(Event):
    """ Asset event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        account: Trading account name, e.g. test@gmail.com.
        assets: Asset details.
        timestamp: Publish time, millisecond.
        update: If any update in this publish.

    * NOTE:
        Publisher: Asset server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, account=None, assets=None, timestamp=None, update=False):
        """Initialize."""
        name = "EVENT_ASSET"
        exchange = "Asset"
        routing_key = "{platform}.{account}".format(platform=platform, account=account)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "account": account,
            "assets": assets,
            "timestamp": timestamp,
            "update": update
        }
        super(EventAsset, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        asset = Asset(**self.data)
        return asset


class EventOrder(Event):
    """ Order event.

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        account: Trading account name, e.g. test@gmail.com.
        strategy: Strategy name, e.g. my_test_strategy.
        order_no: order id.
        symbol: Trading pair name, e.g. ETH/BTC.
        action: Trading side, BUY/SELL.
        price: Order price.
        quantity: Order quantity.
        remain: Remain quantity that not filled.
        status: Order status.
        avg_price: Average price that filled.
        order_type: Order type, only for future order.
        ctime: Order create time, millisecond.
        utime: Order update time, millisecond.

    * NOTE:
        Publisher: Strategy Server.
        Subscriber: Any Servers who need.
    """

    def __init__(self, platform=None, account=None, strategy=None, order_no=None, symbol=None, action=None, price=None,
                 quantity=None, remain=None, status=None, avg_price=None, order_type=None, trade_type=None, ctime=None,
                 utime=None):
        """Initialize."""
        name = "EVENT_ORDER"
        exchange = "Order"
        routing_key = "{platform}.{account}.{strategy}".format(platform=platform, account=account, strategy=strategy)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "account": account,
            "strategy": strategy,
            "order_no": order_no,
            "action": action,
            "order_type": order_type,
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "remain": remain,
            "status": status,
            "avg_price": avg_price,
            "trade_type": trade_type,
            "ctime": ctime,
            "utime": utime
        }
        super(EventOrder, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        """ Parse self._data to Order object.
        """
        order = Order(**self.data)
        return order


class EventKline(Event):
    """ Kline event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        open: Open price.
        high: Highest price.
        low: Lowest price.
        close: Close price.
        volume: Trade volume.
        timestamp: Publish time, millisecond.
        kline_type: Kline type, kline/kline_5min/kline_15min.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, open=None, high=None, low=None, close=None, volume=None,
                 timestamp=None, kline_type=None):
        """Initialize."""
        if kline_type == const.MARKET_TYPE_KLINE:
            name = "EVENT_KLINE"
            exchange = "Kline"
        elif kline_type == const.MARKET_TYPE_KLINE_5M:
            name = "EVENT_KLINE_5MIN"
            exchange = "Kline.5min"
        elif kline_type == const.MARKET_TYPE_KLINE_15M:
            name = "EVENT_KLINE_15MIN"
            exchange = "Kline.15min"
        elif kline_type == const.MARKET_TYPE_KLINE_30M:
            name = "EVENT_KLINE_30MIN"
            exchange = "Kline.30min"
        elif kline_type == const.MARKET_TYPE_KLINE_1H:
            name = "EVENT_KLINE_1H"
            exchange = "Kline.1h"
        elif kline_type == const.MARKET_TYPE_KLINE_4H:
            name = "EVENT_KLINE_4H"
            exchange = "Kline.4h"
        elif kline_type == const.MARKET_TYPE_KLINE_12H:
            name = "EVENT_KLINE_12H"
            exchange = "Kline.12h"
        elif kline_type == const.MARKET_TYPE_KLINE_24H:
            name = "EVENT_KLINE_24H"
            exchange = "Kline.24h"
        else:
            logger.error("kline_type error! kline_type:", kline_type, caller=self)
            return
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": timestamp,
            "kline_type": kline_type
        }
        super(EventKline, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        kline = Kline(**self.data)
        return kline


class EventOrderbook(Event):
    """ Orderbook event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        asks: Asks, e.g. [[price, quantity], ... ]
        bids: Bids, e.g. [[price, quantity], ... ]
        timestamp: Publish time, millisecond.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, asks=None, bids=None, timestamp=None):
        """Initialize."""
        name = "EVENT_ORDERBOOK"
        exchange = "Orderbook"
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": timestamp
        }
        super(EventOrderbook, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        orderbook = Orderbook(**self.data)
        return orderbook


class EventTrade(Event):
    """ Trade event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        action: Trading side, BUY or SELL.
        price: Order price.
        quantity: Order size.
        timestamp: Publish time, millisecond.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, action=None, price=None, quantity=None, timestamp=None):
        """ 初始化
        """
        name = "EVENT_TRADE"
        exchange = "Trade"
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "timestamp": timestamp
        }
        super(EventTrade, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        trade = Trade(**self.data)
        return trade


class EventStart(Event):
    """docstring for Event"""

    def __init__(self, robot_id):
        routing_key = "{robot_id}".format(robot_id=robot_id)
        exchange = 'start'
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "robot_id": robot_id
        }
        super(EventStart, self).__init__(name='HANDLE_START', exchange=exchange, routing_key=routing_key, queue=queue, data=data)

    def parse(self):
        """ 解析self._data数据
        """
        return self.data

    def subscribe(self, callback, multi=False):
        """ 订阅此事件
        @param callback 回调函数
        @param multi 是否批量订阅消息，即routing_key为批量匹配
        """
        from quant.quant import quant
        self._callback = callback
        SingleTask.run(quant.handle_event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """ 发布此事件
        """
        from quant.quant import quant
        SingleTask.run(quant.handle_event_center.publish, self)


class EventStop(Event):
    """docstring for """

    def __init__(self, robot_id):
        routing_key = "{robot_id}".format(robot_id=robot_id)
        exchange = 'stop'
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "robot_id": robot_id
        }
        super(EventStop, self).__init__(name='HANDLE_STOP', exchange='stop', queue=queue, routing_key=routing_key, data=data)

    def parse(self):
        """ 解析self._data数据
        """
        return self.data

    def subscribe(self, callback, multi=False):
        """ 订阅此事件
        @param callback 回调函数
        @param multi 是否批量订阅消息，即routing_key为批量匹配
        """
        from quant.quant import quant
        self._callback = callback
        SingleTask.run(quant.handle_event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """ 发布此事件
        """
        from quant.quant import quant
        SingleTask.run(quant.handle_event_center.publish, self)


class EventEditVar(Event):
    def __init__(self, robot_id, value=None, name=None, kind=None):
        routing_key = "{robot_id}".format(robot_id=robot_id)
        exchange = 'edit_var'
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "robot_id": robot_id,
            "value": value,
            "name": name,
            "kind": kind
        }

        super(EventEditVar, self).__init__(name='HANDLE_EDIT_VAR', exchange=exchange, queue=queue, routing_key=routing_key,
                                           data=data)

    def parse(self):
        """ 解析self._data数据
        """
        editvar = EditVar(**self.data)
        return editvar

    def subscribe(self, callback, multi=False):
        """ 订阅此事件
        @param callback 回调函数
        @param multi 是否批量订阅消息，即routing_key为批量匹配
        """
        from quant.quant import quant
        self._callback = callback
        SingleTask.run(quant.handle_event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """ 发布此事件
        """
        from quant.quant import quant
        SingleTask.run(quant.handle_event_center.publish, self)


class EventCenter:
    """ Event center.
    """

    def __init__(self):
        self._host = config.rabbitmq.get("host", "localhost")
        self._port = config.rabbitmq.get("port", 5672)
        self._virtualhost = config.rabbitmq.get("virtualhost", '/')
        self._username = config.rabbitmq.get("username", "guest")
        self._password = config.rabbitmq.get("password", "guest")
        self._protocol = None
        self._channel = None  # Connection channel.
        self._connected = False  # If connect success.
        self._subscribers = []  # e.g. [(event, callback, multi), ...]
        self._event_handler = {}  # e.g. {"exchange:routing_key": [callback_function, ...]}

        # Register a loop run task to check TCP connection's healthy.
        LoopRunTask.register(self._check_connection, 10)

    def initialize(self):
        asyncio.get_event_loop().run_until_complete(self.connect())

    @async_method_locker("EventCenter.subscribe")
    async def subscribe(self, event: Event, callback=None, multi=False):
        """ Subscribe a event.

        Args:
            event: Event type.
            callback: Asynchronous callback.
            multi: If subscribe multiple channel(routing_key) ?
        """
        logger.info("NAME:", event.name, "EXCHANGE:", event.exchange, "QUEUE:", event.queue, "ROUTING_KEY:",
                    event.routing_key, caller=self)
        self._subscribers.append((event, callback, multi))

    async def publish(self, event):
        """ Publish a event.

        Args:
            event: A event to publish.
        """
        if not self._connected:
            logger.warn("RabbitMQ not ready right now!", caller=self)
            return
        data = event.dumps()
        await self._channel.basic_publish(payload=data, exchange_name=event.exchange, routing_key=event.routing_key)

    async def connect(self, reconnect=False):
        """ Connect to RabbitMQ server and create default exchange.

        Args:
            reconnect: If this invoke is a re-connection ?
        """
        logger.info("host:", self._host, "port:", self._port, caller=self)
        if self._connected:
            return

        # Create a connection.
        try:
            transport, protocol = await aioamqp.connect(host=self._host, port=self._port, login=self._username,
                                                        password=self._password, login_method="PLAIN", virtualhost=self._virtualhost)
        except Exception as e:
            logger.error("connection error:", e, caller=self)
            return
        finally:
            if self._connected:
                return
        channel = await protocol.channel()
        self._protocol = protocol
        self._channel = channel
        self._connected = True
        logger.info("Rabbitmq initialize success!", caller=self)

        # Create default exchanges.
        exchanges = ["Orderbook", "Trade", "Kline", "Kline.5min", "Kline.15min", "Config", "Heartbeat", "Asset",
                     "Order", "Position", "PreWarning.asset_warning", "PreWarning.market_warning", "PreWarning.position_warning",
                     "Kline.30min", "Kline.1h", "Kline.4h", "Kline.12h", "Kline.12h", "Kline.24h"]
        for name in exchanges:
            await self._channel.exchange_declare(exchange_name=name, type_name="topic")
        logger.debug("create default exchanges success!", caller=self)

        if reconnect:
            self._bind_and_consume()
        else:
            # Maybe we should waiting for all modules to be initialized successfully.
            asyncio.get_event_loop().call_later(5, self._bind_and_consume)

    def _bind_and_consume(self):
        async def do_them():
            for event, callback, multi in self._subscribers:
                await self._initialize(event, callback, multi)
        SingleTask.run(do_them)

    async def _initialize(self, event: Event, callback=None, multi=False):
        if event.queue:
            await self._channel.queue_declare(queue_name=event.queue, auto_delete=True)
            queue_name = event.queue
        else:
            result = await self._channel.queue_declare(exclusive=True)
            queue_name = result["queue"]
        await self._channel.queue_bind(queue_name=queue_name, exchange_name=event.exchange,
                                       routing_key=event.routing_key)
        await self._channel.basic_qos(prefetch_count=event.prefetch_count)
        if callback:
            if multi:
                await self._channel.basic_consume(callback=callback, queue_name=queue_name, no_ack=True)
                logger.info("multi message queue:", queue_name, caller=self)
            else:
                await self._channel.basic_consume(self._on_consume_event_msg, queue_name=queue_name)
                logger.info("queue:", queue_name, caller=self)
                self._add_event_handler(event, callback)

    async def _on_consume_event_msg(self, channel, body, envelope, properties):
        # logger.debug("exchange:", envelope.exchange_name, "routing_key:", envelope.routing_key,
        #              "body:", body, caller=self)
        try:
            key = "{exchange}:{routing_key}".format(exchange=envelope.exchange_name, routing_key=envelope.routing_key)
            funcs = self._event_handler[key]
            for func in funcs:
                SingleTask.run(func, channel, body, envelope, properties)
        except:
            logger.error("event handle error! body:", body, caller=self)
            return
        finally:
            await self._channel.basic_client_ack(delivery_tag=envelope.delivery_tag)  # response ack

    def _add_event_handler(self, event: Event, callback):
        key = "{exchange}:{routing_key}".format(exchange=event.exchange, routing_key=event.routing_key)
        if key in self._event_handler:
            self._event_handler[key].append(callback)
        else:
            self._event_handler[key] = [callback]
        logger.debug("event handlers:", self._event_handler.keys(), caller=self)

    async def _check_connection(self, *args, **kwargs):
        if self._connected and self._channel and self._channel.is_open:
            logger.debug("RabbitMQ connection ok.", caller=self)
            return
        logger.error("CONNECTION LOSE! START RECONNECT RIGHT NOW!", caller=self)
        self._connected = False
        self._protocol = None
        self._channel = None
        self._event_handler = {}
        SingleTask.run(self.connect, reconnect=True)


class EventHandleCenter(EventCenter):
    """ 事件处理中心
    """

    def __init__(self):
        self._host = config.handle_rabbitmq.get("host", "localhost")
        self._port = config.handle_rabbitmq.get("port", 5672)
        self._username = config.handle_rabbitmq.get("username", "guest")
        self._password = config.handle_rabbitmq.get("password", "guest")
        self._protocol = None
        self._channel = None  # 连接通道
        self._connected = False  # 是否连接成功
        self._subscribers = []  # 订阅者 [(event, callback, multi), ...]
        self._event_handler = {}  # 事件对应的处理函数 {"exchange:routing_key": [callback_function, ...]}

        LoopRunTask.register(self._check_connection, 10)  # 检查连接是否正常

    async def connect(self, reconnect=False):
        """ 建立TCP连接
        @param reconnect 是否是断线重连
        """
        logger.info("host:", self._host, "port:", self._port, caller=self)
        if self._connected:
            return

        # 建立连接
        try:
            transport, protocol = await aioamqp.connect(host=self._host, port=self._port, login=self._username,
                                                        password=self._password)
        except Exception as e:
            logger.error("connection error:", e, caller=self)
            return
        finally:
            # 如果已经有连接已经建立好，那么直接返回（此情况在连续发送了多个连接请求后，若干个连接建立好了连接）
            if self._connected:
                return
        channel = await protocol.channel()
        self._protocol = protocol
        self._channel = channel
        self._connected = True
        logger.info("Rabbitmq initialize success!", caller=self)

        # 创建默认的交换机
        exchanges = ["start", "stop", "edit_var", ]
        for name in exchanges:
            await self._channel.exchange_declare(exchange_name=name, type_name="topic")
        logger.debug("create default exchanges success!", caller=self)

        # 如果是断线重连，那么直接绑定队列并开始消费数据，如果是首次连接，那么等待5秒再绑定消费（等待程序各个模块初始化完成）
        if reconnect:
            self._bind_and_consume()
        else:
            asyncio.get_event_loop().call_later(5, self._bind_and_consume)


class EventPosition(Event):
    """仓位更新事件
    *NOTE:
    发布：资产服务
    订阅：业务模块
    """
    def __init__(self, platform=None, account=None, positions=None, timestamp=None):
        """
        初始化
        :param platform: 平台
        :param account: 账号
        :param positions: 仓位
        :param timestamp: 时间戳
        """
        name = "EVENT_POSITION"
        exchange = "Position"
        routing_key = "{platform}.{account}".format(platform=platform, account=account)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)

        data = {
            "platform": platform,
            "account": account,
            "positions": positions,
            "timestamp": timestamp
        }
        super(EventPosition, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        """ 解析self._data数据
        """
        position = PositionObject(**self.data)
        return position


class EventPreWarning(Event):
    """
    预警事件
    """
    def __init__(self, platform=None, strategy=None, pre_warning=None, warning_type=None, timestamp=None):
        name = 'EVENT_PREWARNING'
        exchange = "PreWarning.{}".format(warning_type)
        routing_key = "{platform}.{strategy}".format(platform=platform, strategy=strategy)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)

        data = {
            "platform": platform,
            "strategy": strategy,
            "warning": pre_warning,
            "warning_type": warning_type,
            "timestamp": timestamp
        }
        super(EventPreWarning, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        """ 解析self._data数据
        """
        pre_warning = PreWarning(**self.data)
        return pre_warning