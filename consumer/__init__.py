from .__main__ import *

if __name__ == '__main__':
    consumer = Consumer()
    message = consumer.consume()
    print(message)
