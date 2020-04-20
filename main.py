import sys
from quant.config import config



def initialize():
    if config.strategy == 'test1':
        from strategy import Test
        Test()



def main():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None

    from quant.quant import quant

    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == '__main__':
    main()
