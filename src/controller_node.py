import socket
import threading
import json

import time

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

NODES = []
receive_ack =[]

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
        pass
    elif data["type"] == "client_stop":
        pass
    elif data["type"] == "ack_playback":
        handle_playback_ack(data)
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

def send_request_to_node(node, playback_message):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((node['HOST'], node['PORT']))
        s.sendall(json.dumps(playback_message).encode('utf-8'))
        print(f"Sent playback initiation to {node['NODE_ID']}")

        # Wait for acknowledgment
        response = s.recv(1024)
        if not response:
            print(f"No response from node {node['NODE_ID']}")
            return

        try:
            response_data = json.loads(response.decode('utf-8'))
            receive_ack.append(response_data.get("answer", "no"))
            print(f"Received acknowledgment from {node['NODE_ID']}: {response_data}")
        except json.JSONDecodeError as e:
            print(f"Invalid JSON response from {node['NODE_ID']}: {response}. Error: {e}")
        s.close()
    except socket.error as e:
        print(f"Error communicating with {node['NODE_ID']}: {e}")

def initiate_playback(content_id, action, scheduled_time):
    time.sleep(10)
    global NODES
    global receive_ack
    receive_ack=[]
    print (NODES, "nodess")

    print(f"Initiating playback: {action} for content {content_id} at {scheduled_time}")
    playback_message = {
        "type": "init_playback",
        "sender_id": "controller",
        "message_id": "msg-init-playback",
        "timestamp": time.time(),
        "action": action,
        "content_id": content_id,
        "scheduled_time": scheduled_time
    }

    # Create a thread for each node
    threads = []
    for node in NODES:
        thread = threading.Thread(target=send_request_to_node, args=(node, playback_message))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    
          
def handle_playback_ack(data):
    global receive_ack
    global NODES
    receive_ack.append(data["answer"])

    # Check if enough nodes are ready for playback
    ready_count = sum(1 for ack in receive_ack if ack == "yes")
    print(f"Ready nodes: {ready_count}/{len(NODES)}")

    if ready_count == (len(NODES)):  # Check quorum
        confirm_playback(data["content_id"], data["action"], data["scheduled_time"])
        # Reschedule the function to run again after 10 seconds
        threading.Timer(10, initiate_playback, args=("video456", "play", time.time() + 10)).start()
    else:
        print("Not enough nodes are ready for playback. Cancelling playback.")

def confirm_playback(content_id, action, scheduled_time):
    global NODES

    print(f"Confirming playback: {action} for content {content_id} at {scheduled_time}")
    confirmation_message = {
        "type": "confirm_playback",
        "sender_id": "controller",
        "message_id": "msg-confirm-playback",
        "timestamp": time.time(),
        "action": action,
        "content_id": content_id,
        "scheduled_time": scheduled_time
    }

    # Broadcast confirmation message to all nodes
    for node in NODES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((node['HOST'], node['PORT']))
            s.sendall(json.dumps(confirmation_message).encode('utf-8'))
            s.close()
        except socket.error as e:
            print(f"Error sending confirmation to {node['NODE_ID']}: {e}")



if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(CONTROLLER_HOST, CONTROLLER_PORT))
    listener_thread.start()

    listener_thread = threading.Thread(target=initiate_playback, args=("video123", "play", time.time() + 10))
    listener_thread.start()