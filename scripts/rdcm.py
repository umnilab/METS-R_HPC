import websocket
import websockets
import asyncio
import json
import sys
import threading

def test_connection(uri):
    async def test_client():
        try:
            async with websockets.connect(uri) as websocket:
                return True

        except:
            return False

    return asyncio.new_event_loop().run_until_complete(test_client())


class RDClient(threading.Thread):

    def __init__(self, uri, index, msg_log_size=20):
        super().__init__()
        self.ws = websocket.WebSocketApp(uri,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        self.uri =  uri
        self.index = index
        self.msg_log_size = msg_log_size

    def on_message(self, ws, message):
        print(f"{self.uri} : {message[0:self.msg_log_size]}")

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print(f"{self.uri} : connection closed")

    def on_open(self, ws):
        print(f"{self.uri} : connection opened")

    def run(self):
        print(f"waiting until the server is up at {self.uri}")
        while  not test_connection(self.uri):
            pass

        print(f"sever is active at {self.uri},  running client..")  
        self.ws.run_forever()

if __name__ == "__main__":

     
    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    num_clients = int(config['num_sim_instances'])
    port_numbers = config['socket_port_numbers']
    rd_clients = []

    for i in range(num_clients):
        client_uri = f"ws://localhost:{port_numbers[i]}"
        ws_client = RDClient(client_uri, i)
        ws_client.start()
        rd_clients.append(ws_client)

    print("created all clients!")  
    for i in range(num_clients):
        rd_clients[i].join()
    
