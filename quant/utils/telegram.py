# -*- coding:utf-8 -*-

"""
Telegram Bot
https://core.telegram.org/api

Author: HuangTao
Date:   2018/12/04
Email:  huangtao@ifclover.com
"""

from quant.utils.http_client import AsyncHttpRequests


class TelegramBot:
    """ Telegram Bot.
    """
    BASE_URL = "https://api.telegram.org"

    @classmethod
    async def send_text_msg(cls, token, chat_id, content):
        """ Send text message.

        Args:
            token: Telegram bot token.
            chat_id: Telegram chat id.
            content: The message string that you want to send.
        """
        url = "{base_url}/bot{token}/sendMessage?chat_id={chat_id}&text={content}".format(
            base_url=cls.BASE_URL,
            token=token,
            chat_id=chat_id,
            content=content
        )
        await AsyncHttpRequests.fetch("GET", url)
