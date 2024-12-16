# DistSys-project
Distributed systems group 17 project.

Shared state synchronized playback system

In this prototype the video playback is substituted with printouts to show when playback is started or stopped.

To start the system use:
```bash
python node.py <ip> <port> 
```
And set corresponding values in the config.json file.
The host needs to be set for proper initialization.

System can be used with following commands using the nc tool with command of following type:
```bash
echo '{"type": "client_play", "HOST": "svm-11-3.cs.helsinki.fi", "PORT": 9940, "NODE_ID": "node-1", "content_id": "file1", "action": "play", "time_after": "10"}' | nc <node_ip> <node_port>
```

```bash
echo '{"type": "client_stop"}' | nc <node_ip> <node_port>
```
