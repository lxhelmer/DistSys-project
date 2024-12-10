import socket
import threading
import json
import time
import uuid
import sys
import tempfile
import os

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

CONTROLLER_HOST = config['CONTROLLER_HOST']
CONTROLLER_PORT = config['CONTROLLER_PORT']
CONTROLLER_ID = config['CONTROLLER_ID']
# NODE_HOST = config['NODE_HOST']
# NODE_PORT = config['NODE_PORT']
# NODE_ID = config['NODE_ID']
NODE_HOST = sys.argv[1]
NODE_PORT = int(sys.argv[2])
NODE_ID = sys.argv[3]
IS_CONTROLLER = False
HEALTH_CHECK_TIMEOUT = config['HEALTH_CHECK_TIMEOUT']
TIME_BETWEEN_HEALTH_CHECKS = config['TIME_BETWEEN_HEALTH_CHECKS']
ELECTION_STARTED = False
STOP_ELECTION = False
ELECTION_DATA = {}

NODES = []

def handle_client_connection(client_socket):
    continue_read = True
    try:
        while continue_read:
            data = client_socket.recv(1024)
            if not data:
                break
            message = json.loads(data.decode('utf-8'))
            continue_read = read_data(message, client_socket)
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()


def read_data(data, client_socket: socket.socket):
    global ELECTION_STARTED
    global STOP_ELECTION
    global ELECTION_DATA
    if data["type"] == "join_system": # received only by the controller node
        print("Received join request from", data)
        append_node_to_list(data)
        reply_with_node_details(client_socket)
        # return False
    elif data["type"] == "join_ack":
        update_nodes_list(data)
        send_discover_to_all_nodes()
    elif data["type"] == "discover_node":
        NODES.append({'HOST': data["HOST"], 'PORT': data["PORT"], 'NODE_ID': data["NODE_ID"]})
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
        pass
    elif data["type"] == "ack_playback":
        pass
    elif data["type"] == "confirm_playback":
        pass
    elif data["type"] == "health_check":
        send_health_ack(data, client_socket)
    elif data["type"] == "leader_election":
        ELECTION_STARTED = True
        handle_leader_election(data, client_socket)
    elif data["type"] == "leader_exists":
        update_leader_details(data)
        ELECTION_DATA[data["ELECTION_ID"]]["status"] = "cancelled"
        print("Stopping leader election due to already existing leader", data)
    elif data["type"] == "leader_nack":
        ELECTION_DATA[data["ELECTION_ID"]]["status"] = "cancelled"
        print("Stopping leader election due to high priority neighbor", data)
    elif data["type"] == "leader_elected":
        print("Received leader elected message", data)
        update_leader_details(data)
        health_check_thread = threading.Thread(target=perform_health_check)
        health_check_thread.start()
        ELECTION_DATA[data["ELECTION_ID"]] = {"status": "completed"}
    else:
        print("Unidentified message")
    return True

def update_leader_details(data):
    global CONTROLLER_HOST, CONTROLLER_PORT, CONTROLLER_ID
    CONTROLLER_HOST = data["HOST"]
    CONTROLLER_PORT = data["PORT"]
    CONTROLLER_ID = data["NODE_ID"]
    print("Leader has been changed", CONTROLLER_HOST, CONTROLLER_PORT, CONTROLLER_ID)

def handle_leader_election(data, client_socket):
    global health_check_thread
    # global ELECTION_DATA
    # ELECTION_DATA[data["ELECTION_ID"]] = "cancelled"
    if IS_CONTROLLER:
        print("Notifying candidate about leader existence", data)
        client_socket.send(json.dumps({"type": "leader_exists", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "ELECTION_ID": data["ELECTION_ID"]}).encode('utf-8'))
    else:
        print("Taking over leader election process", data["ELECTION_ID"])
        client_socket.send(json.dumps({"type": "leader_nack", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "ELECTION_ID": data["ELECTION_ID"]}).encode('utf-8'))
        start_leader_election()

def send_discover_to_all_nodes():
    global health_check_thread
    for node in NODES:
        listener_thread = threading.Thread(target=send_discover_to_node, args=(node,))
        listener_thread.start()
    health_check_thread = threading.Thread(target=perform_health_check)
    health_check_thread.start()

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
    # handle_client_connection(s)
    print("Writing data to file", NODES)
    with open(tempfile.gettempdir() + "/" + NODE_ID + ".json", "w") as f:
        json.dump({"NODES": NODES}, f)

def handle_discover_ack(data):
    print("Received discover ack", data)

def send_health_ack(data, client_socket):
    print("Received health check", data)
    client_socket.send(json.dumps({"type": "health_ack", "NODE_ID": NODE_ID}).encode('utf-8'))

def reply_with_node_details(client_socket: socket.socket):
    client_socket.send(json.dumps({"type": "join_ack", "node_details": NODES}).encode('utf-8'))

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
    with open(tempfile.gettempdir() + "/" + NODE_ID + ".json", "w") as f:
        json.dump({"NODES": NODES}, f)

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

def append_node_to_list(node_info):
    global NODES
    if node_info not in NODES:
        NODES.append(node_info["node_details"])
        print(f"Node added: {node_info}")
    else:
        print(f"Node already in list: {node_info}")
    with open(tempfile.gettempdir() + "/" + NODE_ID + ".json", "w") as f:
        json.dump({"NODES": NODES}, f)

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
        handle_client_connection(s)
    except socket.error as e:
        print(f"Socket error: {e}")

def perform_health_check():
    global ELECTION_DATA
    current_controller_id = CONTROLLER_ID
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
    while True:
        for key, value in ELECTION_DATA.items():
            if value["status"] == "started":
                print(f"An election {key} has been started. Hence aborting existing health check")
                return
        if current_controller_id != CONTROLLER_ID:
            return
        time.sleep(TIME_BETWEEN_HEALTH_CHECKS)
        s.send(json.dumps({"type": "health_check", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID}).encode('utf-8'))
        time.sleep(HEALTH_CHECK_TIMEOUT)
        data = s.recv(1024)
        if not data:
            print("Leader timeout occurred. Starting leader election")
            start_leader_election()
            return
        else:
            message = json.loads(data.decode('utf-8'))
            print("Received health details from leader", message)

def start_leader_election():
    global STOP_ELECTION
    global ELECTION_DATA

    for key, value in ELECTION_DATA.items():
        if value["status"] == "started" and value["owner"] == NODE_ID:
            print("Skipping election due to ongoing election", key)
            return

    election_id = str(uuid.uuid1())
    ELECTION_DATA[election_id] = {"owner": NODE_ID, "status": "started"}
    for node in NODES:
        if int(node["NODE_ID"].split("-")[1]) < int(NODE_ID.split("-")[1]):
            sender_thread = threading.Thread(target=send_leader_election_message, args=(node["HOST"], node["PORT"], node["NODE_ID"], election_id))
            sender_thread.start()
    time.sleep(5)
    if ELECTION_DATA[election_id]["status"] != "started":
        print("Aborting election", election_id)
        return
    STOP_ELECTION = False
    print("Becoming leader", NODES)
    for node in NODES:
        print("Sending leader elected msg", node, election_id)
        sender_thread = threading.Thread(target=send_new_leader_elected_message, args=(node, election_id))
        sender_thread.start()

def send_new_leader_elected_message(node, election_id):
    global ELECTION_DATA
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((node["HOST"], node["PORT"]))
        s.send(json.dumps({"type": "leader_elected", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "ELECTION_ID": election_id}).encode('utf-8'))
        ELECTION_DATA[election_id]["status"] = "completed"
    except socket.error as e:
        print(f"Error connecting with node {node["NODE_ID"]}. Hence ignoring")

def send_leader_election_message(node_host, node_port, node_id, election_id):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((node_host, node_port))
        s.send(json.dumps({"type": "leader_election", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "ELECTION_ID": election_id}).encode('utf-8'))
        print("Sending leader election message to", node_host, node_port, node_id)
        handle_client_connection(s)
    except socket.error as e:
        print(f"Error connecting with node {node_id}. Hence ignoring")

if __name__ == '__main__':
    system_details = {}
    if os.path.isfile(tempfile.gettempdir() + "/" + NODE_ID + ".json"):
        with open(tempfile.gettempdir() + "/" + NODE_ID + ".json") as f:
            system_details = json.load(f)
    listener_thread = threading.Thread(target=listen_for_connection, args=(NODE_HOST, NODE_PORT))
    listener_thread.start()
    if system_details == {}:
        if CONTROLLER_ID != NODE_ID:
            send_node_info_to_controller()
        else:
            IS_CONTROLLER = True
    else:
        print("Restarting node", NODE_ID)
        NODES = system_details["NODES"]
        if int(CONTROLLER_ID.split("-")[1]) > int(NODE_ID.split("-")[1]):
            start_leader_election()
        else:
            print("Joining the system quietly")

