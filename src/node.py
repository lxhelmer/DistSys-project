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
NODE_HOST = config['NODE_HOST']
NODE_PORT = config['NODE_PORT']
NODE_ID = config['NODE_ID']

CURRENT_ACTION = "dummy"
CURRENT_CONTENT_ID = "dummy"
CURRENT_PLAYBACK_TIME ="dummy"

NODES = []
receive_ack =[]
ready_count=0

# Shared resources
playback_request_thread_completed = threading.Event()  # Event to signal all threads are done
active_playback_request_threads = 0  # Counter for active threads

def handle_client_connection(client_socket):
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            try:
                message = json.loads(data.decode('utf-8'))
                read_data(message, client_socket)
            except json.JSONDecodeError:
                message = data.decode('utf-8')
                print(f"Received message: {message}")
                prompt_for_message()  # Prompt for a new message after receiving one
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()

def read_data(data, client_socket: socket.socket):
    print(data)
    if data["type"] == "join_system": # received only by the controller node. may not need this here
        pass
    elif data["type"] == "join_ack":
        update_nodes_list(data)
        send_discover_to_all_nodes()
        # client_socket.close()
    elif data["type"] == "discover_node":
        send_discover_ack(data)
    elif data["type"] == "discover_ack":
        handle_discover_ack(data)
    elif data["type"] == "client_pause":
        pass
    elif data["type"] == "client_play":
        initiate_playback(data["content_id"], data["action"], data["scheduled_time"], node_id=NODE_ID, node_host=NODE_HOST, node_port=NODE_PORT, NODES_LIST=NODES)
    elif data["type"] == "client_stop":
        pass
    elif data["type"] == "init_playback":
        handle_init_playback(data)
    elif data["type"] == "ack_playback":
        handle_playback_ack(data)
    elif data["type"] == "confirm_playback":
        handle_confirm_playback(data)
    elif data["type"] == "state_update":
        handle_state_update
    else:
        print("Unidentified message")

def send_discover_to_all_nodes():
    for node in NODES:
        listener_thread = threading.Thread(target=send_discover_to_node, args=(node,))
        listener_thread.start()

def send_discover_to_node(node):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((node["HOST"], node["PORT"]))
    print("Sending discover message to node:", node["NODE_ID"])
    s.sendall(json.dumps({"type": "discover_node", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID}).encode('utf-8'))
    handle_client_connection(s)

def send_discover_ack(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((data["HOST"], data["PORT"]))
    print("Received discover message from node:", data["NODE_ID"])
    s.sendall(json.dumps({"type": "discover_ack", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID}).encode('utf-8'))
    handle_client_connection(s)


def handle_discover_ack(data):
    print("received discover ack", data)

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

def update_nodes_list(data):
    global NODES
    node_info = data["node_details"]
    NODES = []
    for node in node_info:
        if node['NODE_ID'] != NODE_ID:
            NODES.append(node)
            #print(f"Node added: {node}")
    else:
        print(f"Node already in list or is self: {node}")
    #print(NODES)
    # prompt_for_message()

def send_node_info_to_controller():
    node_info = {
        'HOST': NODE_HOST,
        'PORT': NODE_PORT,
        'NODE_ID': NODE_ID
    }
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
        s.sendall(json.dumps({"type": "join_system", "node_details": node_info}).encode('utf-8'))
        # data = s.recv(1024)
        # global NODES
        # NODES = json.loads(data.decode('utf-8'))
        # #print(f"Updated NODES list: {NODES}")
        # s.close()
        handle_client_connection(s)
    except socket.error as e:
        print(f"Socket error: {e}")

def prompt_for_message():
    print("Enter the message to send to all nodes: ")
    message = input()
    send_message_to_all_nodes(message)

def send_message_to_all_nodes(message):
    for node in NODES:
        try:
            #print(node)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(message.encode('utf-8'))
            s.close()  # Close the connection after sending the message
            #print(f"Message sent to {node['NODE_ID']}")
        except socket.error as e:
            print(f"Socket error: {e}")
    prompt_for_message()  # Prompt for a new message after sending to all nodes




def share_state_with_neighbors():
    global NODES
    global CURRENT_ACTION
    global CURRENT_CONTENT_ID
    global CURRENT_PLAYBACK_TIME
    state_message = {
        "type": "state_update",
        "node_id": NODE_ID,
        "state": {
            "action": CURRENT_ACTION,
            "content_id": CURRENT_CONTENT_ID,
            "current_time": CURRENT_PLAYBACK_TIME              }
    }

    # Send state to all neighbors
    for node in NODES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(json.dumps(state_message).encode('utf-8'))
            s.close()
            print("sharing own state", state_message)
        except socket.error as e:
            print(f"Error sharing state with {node['NODE_ID']}: {e}")

    threading.Timer(10, share_state_with_neighbors).start()

def handle_state_update(data):
    print(f"State update received from {data['node_id']}: {data['state']}")
    # Compare received state with current state
    if data["state"]["action"] != CURRENT_ACTION or data["state"]["content_id"] != CURRENT_CONTENT_ID:
        print("State inconsistency detected. Resynchronizing...")
       # synchronize_with_state(data["state"])
    threading.Timer(10, share_state_with_neighbors).start()

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(NODE_HOST, NODE_PORT))
    listener_thread.start()

    send_node_info_to_controller()
