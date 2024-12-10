import socket
import threading
import json

import time
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.synchronization_utils import initiate_playback, handle_init_playback, handle_playback_ack, handle_confirm_playback

config_path = os.path.join(current_dir, '..', 'config', 'config.json')

# Open and load the JSON configuration
with open(config_path, 'r') as config_file:
    config = json.load(config_file)
CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

NODES = []
receive_ack =[]
ready_count=0

# Shared resources
playback_request_thread_completed = threading.Event()  # Event to signal all threads are done
active_playback_request_threads = 0  # Counter for active threads
lock = threading.Lock()  # Ensure thread-safe updates to the counter

def handle_client_connection(client_socket):
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            # node_info = json.loads(data.decode('utf-8'))
            message = json.loads(data.decode('utf-8'))
            read_data(message, client_socket)
            # print(f"Received node info: {node_info}")
            # update_nodes_list(node_info)
            # send_nodes_list_to_all()
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()


def read_data(data, client_socket):
    print(data)
    
    if data["type"] == "join_system": # received only by the controller node
        print("Received join request from", data)
        update_nodes_list(data)
        reply_with_node_details(client_socket)
    elif data["type"] == "join_ack":
        print("Received request to pause from the user.")
        update_nodes_list(data["node-list"])
    elif data["type"] == "discover_node":
        print("Received request to pause from the user.")
    elif data["type"] == "discover_ack":
        pass
    elif data["type"] == "client_pause":
        pass
    elif data["type"] == "client_play":
        initiate_playback(data["content_id"], data["action"], data["scheduled_time"], node_id="controller", node_host=CONTROLLER_HOST, node_port=CONTROLLER_PORT)
    elif data["type"] == "client_stop":
        pass
    elif data["type"] == "init_playback":
        handle_init_playback(data)
    elif data["type"] == "ack_playback":
        handle_playback_ack(data)
    elif data["type"] == "confirm_playback":
        handle_confirm_playback(data)
    elif data["type"] == "state_update":
        pass
    else:
        print("Unidentified message")

def reply_with_node_details(client_socket: socket.socket):
    client_socket.send(json.dumps({"type": "join_ack", "node_details": NODES}).encode('utf-8'))
    print("sent data")

def listen_for_connection(host, port):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Listening on {host}:{port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection from {addr}")
            client_thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
            client_thread.start()
    except socket.error as e:
        print(f"Socket error: {e}")

def update_nodes_list(node_info):
    global NODES
    if node_info not in NODES:
        NODES.append(node_info["node_details"])
        print(f"Node added: {node_info}")
    else:
        print(f"Node already in list: {node_info}")

def send_nodes_list_to_all():
    global NODES
    nodes_list_json = json.dumps({"type":"node-list", "node-list": NODES})
    for node in NODES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(nodes_list_json.encode('utf-8'))
            s.close()
            print(f"Sent nodes list to {node['NODE_ID']} at {node['HOST']}:{node['PORT']}")
            print(NODES)
        except socket.error as e:
            print(f"Socket error: {e}")


if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(CONTROLLER_HOST, CONTROLLER_PORT))
    listener_thread.start()
