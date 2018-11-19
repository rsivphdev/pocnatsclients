import asyncio
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout


class Client:
    def __init__(self, nc, loop=asyncio.get_event_loop()):
        self.nc = nc
        self.loop = loop

    @asyncio.coroutine
    def message_handler(self, msg):
        print("[Received on '{}']: {}".format(msg.subject, msg.data.decode()))

    @asyncio.coroutine
    def request_handler(self, msg):
        print("[Request on '{} {}']: {}".format(msg.subject, msg.reply,
                                                msg.data.decode()))
        yield from self.nc.publish(msg.reply, b"I can help!")

    def start(self):
        try:
            yield from self.nc.connect(io_loop=self.loop)
        except:
            pass

        nc = self.nc
        try:
            # Interested in receiving 2 messages from the 'discover' subject.
            yield from nc.publish("discover", b'hello')
            yield from nc.publish("discover", b'world')

            # Following 2 messages won't be received.
            yield from nc.publish("discover", b'again')
            yield from nc.publish("discover", b'!!!!!')
        except ErrConnectionClosed:
            print("Connection closed prematurely")

        # Wait a bit for messages to be dispatched...
        yield from asyncio.sleep(2, loop=self.loop)

        yield from nc.flush(0.500)

        # Detach from the server.
        yield from nc.close()

        if nc.last_error is not None:
            print("Last Error: {}".format(nc.last_error))

        if nc.is_closed:
            print("Disconnected.")


if __name__ == '__main__':
    c = Client(NATS())
    c.loop.run_until_complete(c.start())
    c.loop.close()
