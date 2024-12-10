import socket
import threading
import json
import time 

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']
NODE_HOST = config['NODE_HOST']
NODE_PORT = config['NODE_PORT']
NODE_ID = config['NODE_ID']

NODES = []
CURRENT_ACTION = "dummy"
CURRENT_CONTENT_ID = "dummy"
CURRENT_PLAYBACK_TIME ="dummy"

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
        pass
    elif data["type"] == "client_stop":
        pass
    elif data["type"] == "init_playback":
        handle_init_playback(data)
    elif data["type"] == "ack_playback":
        pass
    elif data["type"] == "confirm_playback":
        handle_confirm_playback(data)
    elif data["type"] == "state_update":
        pass#time.sleep(5)
        #handle_state_update(data)
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

def handle_init_playback(data):
    print(f"Received playback initiation: {data}")

    # Check if the video exists and if the node is ready
    #need to add this will discuss video_exists = check_video(data["content_id"])
    node_ready = True

    # Send acknowledgment back to the initiating node
    ack_message = {
        "type": "ack_playback",
        "sender_id": NODE_ID,
        "message_id": "msg-ack-playback",
        "init_message_id": data["message_id"],
        "timestamp": time.time(),
        "answer": "yes", #if video_exists and node_ready else "no",
        "action":data["action"],
        "content_id":data["content_id"],
        "scheduled_time": data["scheduled_time"]
    }

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
        print(f"Sending acknowledgment: {ack_message}")
        s.sendall(json.dumps(ack_message).encode('utf-8'))
        s.close()
    except socket.error as e:
        print(f"Error sending ack_playback to controller: {e}")

def handle_confirm_playback(data):
    global CURRENT_ACTION
    global CURRENT_PLAYBACK_TIME
    global CURRENT_CONTENT_ID
    print(f"Confirmed playback received: {data}")
    scheduled_time = data["scheduled_time"]

    # Wait until the scheduled time to start playback
    time_to_wait = scheduled_time - time.time()
    if time_to_wait > 0:
        time.sleep(time_to_wait)

    # Execute the playback action
    CURRENT_ACTION=data["action"]
    CURRENT_CONTENT_ID = data["content_id"]
    CURRENT_PLAYBACK_TIME = data["scheduled_time"]
    #execute_playback(data["action"], data["content_id"])  #  Need to add this function on how to run the video
    #for now just printing
    print("Executing the Playback Function.")

def share_state_with_neighbors():
    global NODES
    global CURRENT_ACTION
    global CURRENT_CONTENT_ID
    global CURRENT_PLAYBACK_TIME
    while True:
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

        time.sleep(10)  # Share state every 10 seconds

def handle_state_update(data):
    print(f"State update received from {data['node_id']}: {data['state']}")
    # Compare received state with current state
    if data["state"]["action"] != CURRENT_ACTION or data["state"]["content_id"] != CURRENT_CONTENT_ID:
        print("State inconsistency detected. Resynchronizing...")
       # synchronize_with_state(data["state"])

if __name__ == '__main__':
    listener_thread = threading.Thread(target=listen_for_connection, args=(NODE_HOST, NODE_PORT))
    listener_thread.start()

    send_node_info_to_controller()
    state_sharing_thread = threading.Thread(target=share_state_with_neighbors)
    state_sharing_thread.start()
