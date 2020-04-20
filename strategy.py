from quant.handle import Handle
from quant.const import (
    HANDLE_TYPE_START,
    HANDLE_TYPE_STOP,
    HANDLE_TYPE_EDIT_VAR
)
from quant.handle import EditVar
from quant.config import config
from quant.utils import logger
from quant.utils.decorator import async_method_locker


class Test(object):
    """docstring for Test"""
    def __init__(self):
        
        server_id = config.server_id
        
        Handle(HANDLE_TYPE_START, server_id, self.on_event_start)
        Handle(HANDLE_TYPE_STOP, server_id, self.on_event_stop)
        Handle(HANDLE_TYPE_EDIT_VAR, server_id, self.on_event_edit_var)

        self.spacing = 0

    @async_method_locker("core")
    async def on_event_start(self, data):
        
        logger.info('收到启动命令：', data)

    @async_method_locker("core")
    async def on_event_stop(self, data):
        
        logger.info('收到暂停命令：', data)

    @async_method_locker("core")
    async def on_event_edit_var(self, editVar: EditVar):
        

        setattr(self, editVar.name, editVar.value)
        logger.info('收到修改变量命令, 修改变量: {}, 改为: {}, 变量类型为: {}'.format(editVar.name, editVar.value, editVar.kind))
        if config.edit_var(editVar.name, editVar.value):
            logger.info('修改config本地文件成功')
        else:
            logger.error('修改config本地文件失败')
