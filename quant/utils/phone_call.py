# -*- coding:utf-8 -*-

"""
Aliyun Phone Call API.
https://dyvms.console.aliyun.com/dyvms.htm

Author: HuangTao
Date:   2019/03/22
Email:  huangtao@ifclover.com
"""

import hmac
import base64
import hashlib
from urllib import parse

from quant.utils import tools
from quant.utils.http_client import AsyncHttpRequests


class AliyunPhoneCall:
    """ Aliyun Phone Call API.

    Attributes:
        access_key: Aliyun Access Key.
        secret_key: Aliyun Secret Key.
        _from: Call out phone, eg: 08177112233
        to: Which phone to be called, eg: 13123456789
        code: Phone ring code, e.g. `64096325-d22e-4cf8-9f52-abc12345.wav`
        region_id: Which region to be used, default is `cn-hangzhou`.
    """

    @classmethod
    async def call_phone(cls, access_key, secret_key, _from, to, code, region_id="cn-hangzhou"):
        """ Initialize. """
        def percent_encode(s):
            res = parse.quote_plus(s.encode("utf8"))
            res = res.replace("+", "%20").replace("*", "%2A").replace("%7E", "~")
            return res

        url = "http://dyvmsapi.aliyuncs.com/"
        out_id = tools.get_uuid1()
        nonce = tools.get_uuid1()
        timestamp = tools.dt_to_date_str(tools.get_utc_time(), fmt="%Y-%m-%dT%H:%M:%S.%fZ")

        params = {
            "VoiceCode": code,
            "OutId": out_id,
            "CalledNumber": to,
            "CalledShowNumber": _from,
            "Version": "2017-05-25",
            "Action": "SingleCallByVoice",
            "Format": "JSON",
            "RegionId": region_id,
            "Timestamp": timestamp,
            "SignatureMethod": "HMAC-SHA1",
            "SignatureType": "",
            "SignatureVersion": "1.0",
            "SignatureNonce": nonce,
            "AccessKeyId": access_key
        }
        query = "&".join(["{}={}".format(percent_encode(k), percent_encode(params[k])) for k in sorted(params.keys())])
        str_to_sign = "GET&%2F&" + percent_encode(query)
        h = hmac.new(bytes(secret_key + "&", "utf8"), bytes(str_to_sign, "utf8"), digestmod=hashlib.sha1)
        signature = base64.b64encode(h.digest()).decode()
        params["Signature"] = signature
        await AsyncHttpRequests.fetch("GET", url, params=params)
