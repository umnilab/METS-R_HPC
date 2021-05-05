import websocket
import websockets
import asyncio
import json
import sys
import threading


import socket
from contextlib import closing

# utilities


def check_socket(host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            return True
        else:
            return False

def str_to_int(int_str):
    return int(int_str)

def str_list_to_int_list(str_list):
    return [str_to_int(int_str) for int_str in str_list]
# def test_connection(uri):
#     async def test_client():
#         try:
#             async with websockets.connect(uri) as websocket:
#                 return True

#         except:
#             return False

#     return asyncio.new_event_loop().run_until_complete(test_client())


class RDClient(threading.Thread):

    def __init__(self, host, port, index, msg_log_size=50):
        super().__init__()

        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self.index = index
        self.msg_log_size = msg_log_size

        # data maps

        self.route_ucb_received = {}
        self.route_ucb_bus_received = {}
        self.link_ucb_received = {}
        self.link_ucb_bus_received = {}
        self.speed_vehicle_received = {}

        self.ws = websocket.WebSocketApp(self.uri,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def on_message(self, ws, message):
        print(f"{self.uri} : {message[0:self.msg_log_size]}")

        # decode the json string
        decoded_msg = json.loads(message)

        assert "TYPE" not in decoded_msg.keys(),\
            "received json object does not have TYPE field!"

        if decoded_msg["TYPE"] == "OD_PAIR":
            self.update_route_ucb(decoded_msg)
        elif decoded_msg["TYPE"] == "BOD_PAIR":
            self.update_route_ucb_bus(decoded_msg)
        elif decoded_msg["TYPE"] == "TICK_MSG":
            entries = decoded_msg["entries"]
            for entry in entries:
                pass

    def update_route_ucb(self, json_obj):
        assert json_obj["OD"] in self.route_ucb_received.keys(), \
            f"OD pair {json_obj['OD']} is already in the route_ucb_received map!"

        def str_to_int(int_str):
            return int(int_str)

        def str_list_to_int_list(str_list):
            return [str_to_int(int_str) for int_str in str_list]

        self.route_ucb_received[json_obj["OD"]] = list(map(str_list_to_int_list, json_obj["road_lists"]))

    def update_route_ucb_bus(self, json_obj):
        assert json_obj["BOD"] in self.route_ucb_bus_received.keys(), \
            f"OD pair {json_obj['BOD']} is already in the route_ucb_bus_received map!"

        self.route_ucb_bus_received[json_obj["BOD"]] = list(map(str_list_to_int_list, json_obj["road_lists"]))

    def update_link_ucb_received(self, json_obj):
        pass
    
    def update_link_ucb_bus_received(self, json_obj):
        pass

    def update_speed_vehicle_received(self, json_obj):
        pass

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print(f"{self.uri} : connection closed")

    def on_open(self, ws):
        print(f"{self.uri} : connection opened")

    def run(self):
        print(f"waiting until the server is up at {self.uri}")
        while not check_socket(self.host, self.port):
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
        ws_client = RDClient("localhost", int(port_numbers[i]), i)
        ws_client.start()
        rd_clients.append(ws_client)

    print("created all clients!")
    for i in range(num_clients):
        rd_clients[i].join()
