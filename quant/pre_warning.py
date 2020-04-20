# encoding: utf-8
import json


class PreWarning:
    """
    预警对象
    """
    def __init__(self, platform=None, strategy=None, warning=None, warning_type=None, timestamp=None):
        """

        :param platform: 交易平台
        :param strategy: 策略
        :param pre_warnings: 警告
        :param timestamp:时间戳(毫秒)
        """
        self.platform = platform
        self.strategy = strategy
        self.warning = warning
        self.timestamp = timestamp
        self.warning_type = warning_type

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "strategy": self.strategy,
            "warning": self.warning,
            "timestamp": self.timestamp,
            "warning_type": self.warning_type
        }
        return d

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)


class PreWarningSubscribe:
    """
    预警订阅
    """
    def __init__(self, platform, strategy, warning_type, callback):
        """ 初始化
        @param platform 交易平台
        @param strategy 策略名
        @param callback 资产更新回调函数，必须是async异步函数，回调参数为 PreWarning 对象，比如: async def on_event_prewarning_update(prewarning: PreWarning): pass
        """
        from quant.event import EventPreWarning
        EventPreWarning(platform, strategy, warning_type=warning_type).subscribe(callback)

