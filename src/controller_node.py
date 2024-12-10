import socket
import threading
import json

import time

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']

NODES = []

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
    elif data["type"] == "init_playback":
        pass
    elif data["type"] == "ack_playback":
        pass
    elif data["type"] == "confirm_playback":
        pass
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


def initiate_playback(content_id, action, scheduled_time):
    global NODES
    while True:
        time.sleep(10)
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

        # Send playback initiation message to all nodes
        responses = []
        for node in NODES:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((node['HOST'], node['PORT']))
                s.sendall(json.dumps(playback_message).encode('utf-8'))
                print("a")
                response = s.recv(1024)  # Wait for acknowledgment
                ("got ack=>", response)
                if not response:
                    print(f"No response from node {node['NODE_ID']}")
                    continue
                responses.append(json.loads(response.decode('utf-8')))
                try:
                    response_data = json.loads(response.decode('utf-8'))
                    responses.append(response_data)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON response from node {node['NODE_ID']}: {response}. Error: {e}")
                
                s.close()
            except socket.error as e:
                print(f"Error communicating with {node['NODE_ID']}: {e}")
        
        # Check acknowledgments
        all_ready = [1  for resp in responses if resp["answer"] == "yes"]
        print(all_ready)
        print( (len(responses)/2 ))
        if len(all_ready) >= (len(responses)/2 ):
            confirm_playback(content_id, action, scheduled_time)
        else:
            print("Not all nodes are ready for playback. Cancelling playback.")

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
            print("confirmation message sent")
        except socket.error as e:
            print(f"Error sending confirmation to {node['NODE_ID']}: {e}")



if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(CONTROLLER_HOST, CONTROLLER_PORT))
    listener_thread.start()

    listener_thread = threading.Thread(target=initiate_playback, args=("video123", "play", time.time() + 10))
    listener_thread.start()