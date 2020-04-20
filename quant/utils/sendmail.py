# -*- coding:utf-8 -*-

"""
Send email.

Author: HuangTao
Date:   2018/12/04
Email:  huangtao@ifclover.com
"""

import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import aiosmtplib

from quant.utils import logger


class SendEmail:
    """ Send email.

    Attributes:
        host: Mail server host.
        port: Mail server port.
        username: Email username, e.g. test@gmail.com
        password: Email password.
        to_emails: Email list, which mails to be send.
        subject: Email subject(title).
        content: Email content(body).
        timeout: Send timeout time(seconds), default is 30s.
        tls: If use TLS, default is True.
    """

    def __init__(self, host, port, username, password, to_emails, subject, content, timeout=30, tls=True):
        """ Initialize. """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._to_emails = to_emails
        self._subject = subject
        self._content = content
        self._timeout = timeout
        self._tls = tls

    async def send(self):
        """ Send a email.
        """
        message = MIMEMultipart("related")
        message["Subject"] = self._subject
        message["From"] = self._username
        message["To"] = ",".join(self._to_emails)
        message["Date"] = email.utils.formatdate()
        message.preamble = "This is a multi-part message in MIME format."
        ma = MIMEMultipart("alternative")
        mt = MIMEText(self._content, "plain", "GB2312")
        ma.attach(mt)
        message.attach(ma)

        smtp = aiosmtplib.SMTP(hostname=self._host, port=self._port, timeout=self._timeout, use_tls=self._tls)
        await smtp.connect()
        await smtp.login(self._username, self._password)
        await smtp.send_message(message)
        logger.info("send email success! FROM:", self._username, "TO:", self._to_emails, "CONTENT:", self._content,
                    caller=self)


# if __name__ == "__main__":
#     h = "hwhzsmtp.qiye.163.com"
#     p = 994
#     u = "huangtao@ifclover.com"
#     pw = "123456"
#     t = ["huangtao@ifclover.com"]
#     s = "Test Send Email 测试"
#     c = "Just a test. \n 测试。"
#
#     sender = SendEmail(h, p, u, pw, t, s, c)
#     import asyncio
#     asyncio.get_event_loop().create_task(sender.send())
#     asyncio.get_event_loop().run_forever()
