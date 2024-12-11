# DistSys-project
Distributed systems group 17 project.

## Message types

There are many message types.

- join_system - When a new node joins the system or rejoins after a crash, it sends this message to the central node to get the details about the display nodes and get the video files.
- join_ack - This is the response from the central node to the join system message. The central node will assign an id to the node and will send the node details.
- discover_node - Once the new joinee node gets the node details, it will send a discover node message to all the other nodes to let them know about the new nodes existence.
- discover_ack - Once a new joinee sends a discover message, the receiving node will add the details to its memory and will send an ack to confirm the discovery.
- client_pause - This message comes from the client (user) and indicates to pause the currently playing video.
- client_play - This message comes from the client (user) and indicates to play a specific video.
- client_stop - This message comes from the client (user) and indicates to stop a playing video.
- init_playback - A display node instructs the nodes to check if execution of specified event (play/pause/stop) is possible at specified time.
- ack_playback - The node send a message according to its ability to execute an event at specified time. Id of the initiation message is included in the message so that the initiation node can match the answer.
- confirm_playback - The playback initiation node sends a confirm playback message to all the nodes once all the nodes reply with ack_playback saying they are all ready to play the video at the given timestamp.


To send client messages to a node, we can use `nc HOST PORT` and then provide the message.
```bash
$ nc 127.0.0.1 9091
{"type":"client-pause"}
```