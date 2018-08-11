import json
import queue
import time
from threading import Thread, local

from websocket import create_connection

thread_local = local()


class WebsocketThread:

    def __init__(self, ws, step_func):
        self.ws = ws
        self.step_func = step_func
        self.thread = Thread(target=self.go)
        self.stop_it = False
        self.thread.start()

    def go(self):
        while not self.stop_it:
            self.step_func(self.ws)

    def stop(self):
        self.ws.close()
        self.stop_it = True


class GDAXWebSocketClient:

    def __init__(self):
        self.url = 'wss://ws-feed.gdax.com'
        self.products = []
        self.channels = ['heartbeat']
        self.in_queue = queue.Queue()
        self.listen_thread = None
        self.out_queue = queue.Queue()
        self.command_thread = None
        self.last_heartbeat = None
        self._reconnect()

    def stop(self):
        if self.command_thread:
            self.command_thread.stop()
        if self.listen_thread:
            self.listen_thread.stop()

    def subscribe(self, products, channels):
        self.channels += [channel for channel in channels if channel not in self.channels]
        self.products += [product for product in products if product not in self.products]
        self.in_queue.put({'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels})

    def _reconnect(self):
        print('reconnecting..')
        self.stop()

        ws = create_connection(self.url)
        self.last_heartbeat = time.time()

        self.listen_thread = WebsocketThread(ws, self._listen)
        self.command_thread = WebsocketThread(ws, self._execute_commands)
        if len(self.products) > 0:
            self.subscribe(self.products, self.channels)

    def _execute_commands(self, ws):
        if time.time() - self.last_heartbeat >= 30:
            self._reconnect()
            return

        last_ping = getattr(thread_local, 'last_ping', 0)
        if time.time() - last_ping >= 15:
            ws.ping('keepalive')
            thread_local.last_ping = time.time()

        if self.in_queue.qsize() > 0:
            command = self.in_queue.get()
            ws.send(json.dumps(command).encode('utf-8'))

    def _listen(self, ws):
        try:
            msg = json.loads(ws.recv())
        except Exception as e:
            print('receiving failed: %s' % e)
            self._reconnect()
            return

        print(msg)
        if msg['type'] == 'heartbeat':
            self.last_heartbeat = time.time()
        elif msg['type'] == 'error':
            print('got error message:')
        else:
            self.out_queue.put(msg)


if __name__ == '__main__':
    client = GDAXWebSocketClient()
    client.subscribe(['BTC-USD'], ['matches'])
    time.sleep(30)
    client.stop()
