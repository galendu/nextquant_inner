# encoding: utf-8
from distutils.core import setup


setup(
    name="thenextquant_inner",
    version="0.2.1",
    packages=[
        "quant",
        "quant.utils",
        "quant.platform",
    ],
    description="Asynchronous driven quantitative trading framework.",
    url="https://gitlab.bitqq.vip:81/april/thenextquant_inner.git",
    author="JiSheTechnology",
    author_email="",
    license="MIT",
    keywords=[
        "thenextquant", "quant", "framework", "async", "asynchronous", "digiccy", "digital", "currency",
        "marketmaker", "binance", "okex", "huobi", "bitmex", "deribit", "kraken", "gemini", "kucoin"
    ],
    install_requires=[
        "async-timeout==3.0.1",
        "aiohttp==3.7.4",
        "aioamqp==0.13.0",
        "motor==2.0.0",
        "cryptocompy==0.1.1.dev1",
        "pandas==0.25.1",
        "pycryptodome==3.8.2",
        "requests==2.21.0 "
    ],
)