# Star Replication

A Distributed Object-Storage System for increased read and write throughputs

## Table of Contents

- [Objective](#objective)
- [Setup](#setup)
- [Usage](#usage)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

> Check the DESIGN/ folder for understanding the code architecture and design.

## Objective

We wish to build a distributed object-storage system from scratch and improve the read and write throughput of [`CRAQ: A Distibuted Object-Storage System`](https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf). We ensure that the read and write histories are linearizable.

## Setup

1. Install [Python 3.9](https://www.python.org/downloads/).
2. Create & Activate a python environment `python3.9 -m venv myenv && source myenv/bin/activate`.
3. Install go version >= 1.21 and the installation procedure for the same can be found [here](https://golang.org/doc/install).

## Usage

### Prerequisite Services

We depend on three different services to run a workflow:

  1. `Redis` (The control plane)
  2. `Plasma` (The object store)
  3. `Triton` (The Model Manager)

  We have a `Makefile` that starts all these services for you.
  To start the services, run:

  1. `make redis`
  2. `make objstore`
  3. `make triton` (if needed)

  There is also a script `service_manager.py`.

  1. `python3 service_manager.py start|stop|restart redis|plasma|triton|all`
  starts/stops/restarts the service(s).
  2. `python3 service_manager.py status` will show the current status of the service(s).

Service configuration(ports etc.) can be changed by editing the
`config.py` file or changing `environment variables`.

## Core Library

The library `core/` helps in setting up the server framework, handling connections and 
transfer of messages. We use this library to implement the server side of the system.
The library has the following functionalities:
- `core/message.py` : To define the message format for communication between servers.
    The same format is used by the client to communicate with the server.
    All the messages are of type `JsonMessage` and can contain any number of fields.
- `core/server.py` : Each server process will be an instance of the `Server` class.
    It listens for incoming connections and handles the messages in a separate
    thread.
- `core/network.py` : To establish connections between servers.
    The class `TcpClient` is used to establish a connection with a server.
    It maintains a pool of connections to the server and reuses them.
    The `send` method sends a message to a server and returns the response message from the server. \
    The class `ConnectionStub` maintains all the connections to the servers in the cluster.
    It takes a set of servers it can connect to and establishes connections to all of them.
    It provides an interface to send messages to any server by name in the cluster without 
    worrying about the connection details. It provides the following methods:
    - `send` : To send a message to a server by name and get the response.
    - `initiate_connections` : To establish connections to all the servers in the cluster. 
        This method should be called during the cluster formation before sending any messages.
- `core/cluster.py` : The class `ClusterManager` is used to manage the cluster of servers.
    It starts the servers and establishes connections between them according to a specified topology.
    We have the following parameters in this class:
    - topology: The cluster topology information of the form `dict[ServerInfo, set[ServerInfo]]`
    representing a set of servers each server can send messages to. This information is used to create `ConnectionStub`
    which maintains the connections between the servers.
    - leader_name: We assign one server in the cluster as the leader
    - sock_pool_size: The number of sockets to be maintained in the pool for each server. This directly affects the number of concurrent connections that can be maintained by the server.
- `core/logger.py` : To log the messages and events in the system.

## System Design of Star Replication

### StarClient

The `StarClient` object can be used to send `SET` and `GET` requests to the cluster. The client sends the `SET` and `GET` requests to any random chosen server from the cluster.


The message format for the requests is of type `JsonMessage` and as follows:
- `SET` request: `{"type": "SET", "key": <key>, "value": <value>, "request_id":<request_id>}`
- `GET` request: `{"type": "GET", "key": <key>}`

### StarCluster
We manage a server of clusters. 
The interconnection among the servers are mananged by the ClusterManager. 

![Image](docs/cr-chain.png)

- Topology: For each server we store the set of servers that, this server can 
send messages to.
    ```python3
        topology={
        self.a: {self.b, self.c, self.d, self.e},
        self.b: {self.a, self.c, self.d, self.e},
        self.c: {self.a, self.b, self.d, self.e},
        self.d: {self.a, self.b, self.c, self.e},
        self.e: {self.a, self.b, self.c, self.d}}
    ```
- The `connect` method of `StarCluster` returns a `StarClient` object which can be used to send requests to the cluster.

### StarServer

This extends the `Server` class from the `core` library. It stores the key-value pairs in its local dictionary. `_process_req` is called whenever a message is received by the server. The server processes the request and sends the response back to the client.  The data structures that the server uses are as follows:-
- Local Dictionary (`self.d`): This is a dictionary which stores the key-value pairs.
- Key and Version Number Map (`self.versions`): This is a dictionary that maps the key to a tuple of request id and the version number.
- Key and their Version States (`self.versionState`): This is a map from the keys to the state of the write made, i.e., clean or dirty.
- Buffer/ Scratch Space (`self.buffer`): We store the writes that haven't been yet applied to the server state here. It is a map from (key, request_id) to (version_number, version_state, value).
- Command Queue (`self.command_queue`): Every server has a command queue in which we add any state changes we want to make for the server except for the changes in the buffer.
- Next Version (`self.next_version`): This stores the version numbers that should be next assigned to any write for a particular key. This is maintained by the leader of the cluster.
- Version_Locks (`self.version_locks`): This contains the locks for each key. These locks are used by the leader.
The command queue associated with the server is consumed by the thread,`_cmd_thread` that makes suitable changes to the server state.

#### Processing Requests from Clients and Other Servers
A server may have to process a get or a set request from the client. As mentioned earlier, when a server gets a request, a call to the `_process_req` method is made. Calls to the `_get`, `_set` and the `_ack` method are made on getting a `GET`, `SET`, and `ACK` requests respectively.

#### Handling SET Request
Whenever a server receives a `SET` request, it first applies the writes in the buffer if the server is not the leader. Before forwarding the request to the next server according to the topology, we check if we are the leader, and if yes, we add to the request the version number for the write. We then forward the request to the next server (if any, i.e, if we haven't reached the end of the chain yet) in the chain. If we are the leader, then we wait for the response to flow through the rest of the chain and once we get the same, we `apply` the write to leader's state.
If we are the last server in the chain, then we start sending the acknowledgment in the back chain. 

Note: The writes are ony applied to the state of the leader server directly when a set request is received. Other servers only store the writes in a buffer.

#### Handling GET Request
In Star replication, `GET` request is sent to a random server in the cluster. If the request is received on the leader server, we serve the request with the value from the key-value store. 
If the server is not leader then either of the two cases can happen :- 
- Leader has an empty buffer for that key: An empty buffer for the key means that there are no dirty writes for the key and we directly serve the request from the key-value store.
- Leader has a write corresponding to the key in the buffer: We don't serve the request locally, rather request the leader to give us the most updated value of the key along with the version number of the write made. We process the response from the leader and if the version number of the write is more recent to the one we have in our key-value store, and if we have the request of that write in the buffer then we update the latest write in our key-value store for that key.

#### Handling Ack Request
In Star Replication, `ACK` request are recvd by servers from the fellow servers in the topology. If the acknowledgment has been verified by the leader already, then we apply the write to the state. If not, then we don't apply to the state. We then forward the ack to the previous server in the chain if any.
