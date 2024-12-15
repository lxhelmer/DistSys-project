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

CURRENT_ACTION = "dummy"
CURRENT_CONTENT_ID = "dummy"
CURRENT_PLAYBACK_TIME ="dummy"

NODES = []
receive_ack =[]
ready_count=0

FILES = []

# Shared resources
playback_request_thread_completed = threading.Event()  # Event to signal all threads are done
active_playback_request_threads = 0  # Counter for active threads

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
        print(f"clientSocket error: {e}")
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
        initiate_playback(data["content_id"], data["action"], data["time_after"], node_id=NODE_ID, node_host=NODE_HOST, node_port=NODE_PORT, NODES_LIST=NODES)

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

    elif data["type"] == "file_list_request":
        send_file_list(client_socket)

    elif data["type"] == "file_list":
        print("Received file list:", data["file_list"])
        handle_file_update(data, client_socket)

    elif data["type"] == "file_request":
        print("Received file request for file:", data["file_name"])
        file_name = data["file_name"]
        if file_name in FILES:
            handle_send_file(file_name, client_socket)

    else:
        print("Unidentified message")
    return True

def update_leader_details(data):
    global CONTROLLER_HOST, CONTROLLER_PORT, CONTROLLER_ID
    CONTROLLER_HOST = data["HOST"]
    CONTROLLER_PORT = data["PORT"]
    CONTROLLER_ID = data["NODE_ID"]
    print("Leader has been changed", CONTROLLER_HOST, CONTROLLER_PORT, CONTROLLER_ID)

def send_file_list(client_socket):
    global FILES
    print("Answering file check with", FILES)
    client_socket.send(json.dumps({"type": "file_list", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_list": FILES}).encode('utf-8'))

def handle_file_update(data, client_socket):
    #client_socket.close()
    global FILES
    print("Checking local files against received file list")
    recv_files = sorted(data["file_list"])
    for r_file in recv_files:
        if r_file not in FILES:
            print("file missing:",r_file)
            try:
                print("Create file socket")
                file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                file_socket.connect((CONTROLLER_HOST, CONTROLLER_PORT))
                handle_ask_file(r_file, file_socket)
            except socket.error as e:
                print(f" fileSocket error: {e}")


def handle_send_file(file_name, client_socket):
    with open("../data/"+file_name, "rb") as f:
        while True:
            send_bytes = f.read(1024)
            print("SENDING")
            print(send_bytes)
            if not (send_bytes):
                break
            client_socket.send(send_bytes)
    print("Sent whole file")
    client_socket.close()

def handle_ask_file(file_name, file_socket):
    print("Sending request for missing file", file_name)
    file_socket.send(json.dumps({"type": "file_request", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID, "file_name": file_name}).encode('utf-8'))
    with open("../data/"+file_name, "wb") as f:
        while True:
            recv_bytes = file_socket.recv(1024)
            print("RECEIVING")
            print(recv_bytes)
            if not recv_bytes:
                break
            f.write(recv_bytes)
    f.close()
    print("Received whole file")
    file_socket.close()

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
    global NODES
    NODES.append({'HOST': CONTROLLER_HOST, 'PORT': CONTROLLER_PORT, 'NODE_ID': NODE_ID})
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

def file_update():
    try:
        print("try to match files")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
        s.send(json.dumps({"type": "file_list_request", "HOST": NODE_HOST, "PORT": NODE_PORT, "NODE_ID": NODE_ID}).encode('utf-8'))
        handle_client_connection(s)
    except socket.error as e:
        print(f" fileSocket error: {e}")


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

def check_files():
    global FILES
    FILES = os.listdir('../data')

if __name__ == '__main__':
    check_files()
    print("FILES:", FILES)
    system_details = {}
   # if os.path.isfile(tempfile.gettempdir() + "/" + NODE_ID + ".json"):
   #     with open(tempfile.gettempdir() + "/" + NODE_ID + ".json") as f:
   #         system_details = json.load(f)
    listener_thread = threading.Thread(target=listen_for_connection, args=(NODE_HOST, NODE_PORT))
    listener_thread.start()


    if system_details == {}:
        if CONTROLLER_ID != NODE_ID:
            file_update()
            send_node_info_to_controller()
        else:
            IS_CONTROLLER = True
    else:
        print("Restarting node", NODE_ID)
        NODES = system_details["NODES"]
        if int(CONTROLLER_ID.split("-")[1]) > int(NODE_ID.split("-")[1]):
            start_leader_election()
        else:
            print("Joining the system and checking for consistency")
            send_node_info_to_controller()
