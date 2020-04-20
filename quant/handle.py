import json
from quant import const
from quant.utils import logger


class EditVar:
    """ 策略变量
    """

    def __init__(self, robot_id, name=None, value=None, kind=None):
        """ 初始化
        @param robot_id 机器人id
        @param name 变量key
        @param value 变量value
        @param kind 变量数据类型
        """
        self.robot_id = robot_id
        self.name = name
        self.value = value
        self.kind = kind

    @property
    def data(self):
        d = {
            'robot_id': self.robot_id,
            'name': self.name,
            'value': self.value,
            'kind': self.kind
        }
        return d

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)


class Handle:
    """ 策略交互订阅模块
    """

    def __init__(self, handle_type, robot_id, callback):
        """ 初始化
        @param server_id 服务id
        @param callback 策略交互回调函数, 必须是async异步函数, 回调参数为交互对象
        """
        if handle_type == const.HANDLE_TYPE_START:
            from quant.event import EventStart
            EventStart(robot_id).subscribe(callback)
        elif handle_type == const.HANDLE_TYPE_STOP:
            from quant.event import EventStop
            EventStop(robot_id).subscribe(callback)
        elif handle_type == const.HANDLE_TYPE_EDIT_VAR:
            from quant.event import EventEditVar
            EventEditVar(robot_id).subscribe(callback)
        else:
            logger.error("handle_type error:", handle_type, caller=self)

