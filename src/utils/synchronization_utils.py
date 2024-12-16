import socket
import threading
import json
import time
import urllib.request

NODES = []
receive_ack =[]
ready_count=0

# Shared resources
playback_request_thread_completed = threading.Event()  # Event to signal all threads are done
active_playback_request_threads = 0  # Counter for active threads
lock = threading.Lock()  # Ensure thread-safe updates to the counter

def send_playback_request_to_node(node, playback_message):
    global active_playback_request_threads
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
        s.close()

def initiate_playback(content_id, action, scheduled_time, node_id, node_host, node_port, NODES_LIST):
    global NODES
    global receive_ack
    global active_playback_request_threads
    global playback_request_thread_completed

    receive_ack=[]
    NODES = NODES_LIST

    print(f"Initiating playback: {action} for content {content_id} at {scheduled_time}", time.time())
    playback_message = {
        "type": "init_playback",
        "sender_id": "controller",
        "message_id": "msg-init-playback",
        "timestamp": time.time(),
        "action": action,
        "content_id": content_id,
        "scheduled_time": scheduled_time,
        "NODE_ID":node_id,
        "node_host":node_host,
        "node_port": node_port
    }

    # Create a thread for each node
    threads = []
    for node in NODES_LIST:
        thread = threading.Thread(target=send_playback_request_to_node, args=(node, playback_message))
        thread.start()
        threads.append(thread)

    # Safely increment the counter
        with lock:
            active_playback_request_threads += 1

    if not playback_request_thread_completed.wait(timeout=30):  # Wait up to 10 seconds
        print("Timeout waiting for all threads to complete.")

    print("All threads have completed or timed out!")
    threading.Timer(10, initiate_confirmation, args=(content_id, action, scheduled_time)).start()

def handle_init_playback(data):
    print(f"Received playback initiation: {data}", time.time())

    # Check if the video exists and if the node is ready
    #need to add this will discuss video_exists = check_video(data["content_id"])
    node_ready = True

    # Send acknowledgment back to the initiating node
    ack_message = {
        "type": "ack_playback",
        "sender_id": data["NODE_ID"],
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
        s.connect((data["node_host"], data["node_port"]))
        print(f"Sending acknowledgment: {ack_message}")
        s.sendall(json.dumps(ack_message).encode('utf-8'))
        s.close()
    except socket.error as e:
        print(f"Error sending ack_playback to controller: {e}")

def handle_playback_ack(data):
    global active_playback_request_threads
    global playback_request_thread_completed
    global ready_count
    global receive_ack

    receive_ack.append(data["answer"])
    # Check if enough nodes are ready for playback
    ready_count = sum(1 for ack in receive_ack if ack == "yes")
    print(f"Ready nodes: {ready_count}/{len(NODES)}")
    # Safely decrement the counter
    with lock:
        active_playback_request_threads -= 1
        if active_playback_request_threads == 0:
            playback_request_thread_completed.set()  # Signal that all threads are done  i.e. Received ack form all nodes, or timeout signal from down nodes. 

def initiate_confirmation(content_id, action, scheduled_time):
    global ready_count
    global NODES
    print(ready_count >= (len(NODES) // 2))

    if ready_count >= (len(NODES) // 2):  # Check quorum
        confirm_playback(content_id, action, scheduled_time)
        # Reschedule the function to run again after 10 seconds
        #threading.Timer(10, initiate_playback, args=("video456", "play", time.time() + 10)).start()
    else:
        print("Not enough nodes are ready for playback. Cancelling playback.")

def confirm_playback(content_id, action, scheduled_time):
    global NODES

    print(f"Confirming playback: {action} for content {content_id} at {scheduled_time}", time.time())
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
    handle_confirm_playback(confirmation_message)

def handle_confirm_playback(data):
    global CURRENT_ACTION
    global CURRENT_PLAYBACK_TIME
    global CURRENT_CONTENT_ID
    print(f"Confirmed playback received: {data}")
    scheduled_time = data["scheduled_time"]
    scheduled_time = time.time() + int(scheduled_time)

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
    print("Executing the Playback Function.", time.time())
    contents = urllib.request.urlopen("https://timeapi.io/api/time/current/zone?timeZone=Europe%2FAmsterdam").read()
    print(contents)