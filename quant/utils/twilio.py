# -*- coding:utf-8 -*-

"""
Twilio Phone Call API.
https://www.twilio.com/

Author: HuangTao
Date:   2018/12/04
Email:  huangtao@ifclover.com
"""

from quant.utils.http_client import AsyncHttpRequests


class Twilio:
    """ Twilio Phone Call API.
    """
    BASE_URL = "https://api.twilio.com"

    @classmethod
    async def call_phone(cls, account_sid, token, _from, to, voice_url=None):
        """ Call phone.

        Args:
        account_sid: Twilio account id.
        token: Twilio Auth Token.
        _from: Call out phone, eg: +17173666666
        to: Which phone to be called, eg: +8513123456789
        voice_url: Phone ring url, e.g. http://demo.twilio.com/docs/voice.xml
        """
        url = "https://{account_sid}:{token}@api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls.json".format(
            account_sid=account_sid,
            token=token
        )
        if not voice_url:
            voice_url = "http://demo.twilio.com/docs/voice.xml"
        data = {
            "Url": voice_url,
            "To": to,
            "From": _from
        }
        await AsyncHttpRequests.fetch("POST", url, body=data)
