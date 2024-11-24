

import json
from enum import Enum
import random
import queue
import socket
from typing import Optional, Final, Tuple, List

from core.logger import server_logger
from core.message import JsonMessage, JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo, QueueElement
from collections import defaultdict

from threading import Lock

class RequestType(Enum):
  SET = 1
  GET = 2
  ACK = 3

class KVGetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

class KVSetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message
    assert "val" in self._json_message, self._json_message
    assert "request_id" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def val(self) -> str:
    return self._json_message["val"]

  @property
  def version(self) -> Optional[int]:
    return self._json_message.get("ver")
  
  @property
  def request_id(self) -> str:
    return self._json_message["request_id"]
  
  @property
  def next_chain(self) -> Optional[dict[ServerInfo, Optional[ServerInfo]]]:
    return self._json_message.get("next_chain")
  
  @property
  def prev_chain(self) -> Optional[dict[ServerInfo, Optional[ServerInfo]]]:
    return self._json_message.get("prev_chain")

  @property
  def leader_verif(self) -> Optional[int]:
    return self._json_message.get("leader_verif")

  @leader_verif.setter
  def leader_verif(self, ans: bool) -> None:
    self._json_message['leader_verif'] = ans

  @next_chain.setter
  def next_chain(self, next_chain: dict[ServerInfo, Optional[ServerInfo]]) -> None:
    self._json_message["next_chain"] = next_chain
  
  @prev_chain.setter
  def prev_chain(self, prev_chain: dict[ServerInfo, Optional[ServerInfo]]) -> None:
    self._json_message["prev_chain"] = prev_chain

  @version.setter
  def version(self, ver: int) -> None:
    self._json_message['ver'] = ver
  
  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)


class KVAckRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message
    # assert "val" in self._json_message, self._json_message
    assert "ver" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def val(self) -> str:
    return self._json_message["val"]

  @property
  def version(self) -> Optional[int]:
    return self._json_message.get("ver")
  
  @property
  def request_id(self) -> Optional[int]:
    return self._json_message.get("request_id")
  
  @property
  def leader_verif(self) -> Optional[int]:
    return self._json_message.get("leader_verif")
  
  @property
  def prev_chain(self) -> dict[ServerInfo, Optional[ServerInfo]]:
    return self._json_message["prev_chain"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)

  @leader_verif.setter
  def leader_verif(self, ans: bool) -> None:
    self._json_message['leader_verif'] = ans


class StarServer(Server):
  """Star replication"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
               next_chain: dict[str, Optional[str]],
               prev_chain: dict[str, Optional[str]],
               leader: ServerInfo,
               server_names: List[str]) -> None:
    """
    intializing the star server class
    Attributes
      info: the information related to the given server
      connection_stud: the connection used to communicate with the cluster
      next_chain: the forward chain that has to be followed in case the given server gets a SET request
      prev_chain: the reverse chain corresponding to the forward chain
      leader: the leader of the cluster
    """
    super().__init__(info, connection_stub)
    self.leader: Final[str] = leader.name

    
    self.d: dict[str, str] = defaultdict(lambda: (0)) 
    """
      the key value store
    """

    
    self.versions: defaultdict[str, Tuple[str, Optional[int]]] = defaultdict(lambda: ('0', 0)) 
    """ 
      This dictionary contains the information related to the version of a key-val pair in the store d.
      the information will be stored in the form (request id, version number)
    """

    
    self.buffer: dict[Tuple[str, str], Tuple[Optional[int], str]] = {} 
    """
      This is the buffer space used to store the writes we get at a node which are not applied yet/ not commited yet/ dirty writes
      it maps the (key, request id) to (version number, value), where version number can be empty
    """

    
    self.next_version: dict[str, int] = defaultdict(lambda: (0)) 
    """
      This dictionary maintain the next available version number for a given key, 
      It is used only in the leader server, to assign a version number to a new set request
      it maps key to the next version number available
    """


    
    self.version_locks: dict[str, Lock] = defaultdict(Lock) 
    """
      This dictionary stores the lock corresponding to a key
      This is used only by the leader as it does not use the buffer, but directly write the values to the key value store
      to change this key value store and the state related to the key, in the leader, we have to acquire a lock 
    """

    
    self.next_chain: dict[str, Optional[str]] = next_chain
    """
      The forward chain that has to be followed if the server gets a SET request from a client    
    """
    
    self.prev_chain: dict[str, Optional[str]] = prev_chain
    """
      The reverse of the next_chain for the server
    """

    self.server_names: List[str] = server_names

  def _process_req(self, msg: JsonMessage) -> JsonMessage:
    """
    handles the different types of requests
    """
    if msg.get("type") == RequestType.GET.name:
      return self._get(KVGetRequest(msg))
    elif msg.get("type") == RequestType.SET.name:
      return self._set(KVSetRequest(msg))
    elif msg.get("type") == RequestType.ACK.name:
      return self._ack(KVAckRequest(msg))
    else:
      server_logger.critical("Invalid message type")
      return JsonMessage({"status": "Unexpected type"})

  def _cmd_thread(self) -> None:
    """
    Thread to handle the command queue, 
    it pops a write request from the queue and then applies the write to the key-value store and changes the state, if they are valid writes
    also handling removing the corresponding pending writes from the buffer
    """

    while True:
      wrt=self.command_queue.get()
      if wrt.reqType == "ACK":
        ele = self.buffer.pop((wrt.key, wrt.version[0])) # remove the corresponding request from the buffer 
        if wrt.version[1] >= self.versions[wrt.key][1]: # comparing the version number with the applied version number, to ignore invalid writes
          self.d[wrt.key]=ele[1]
          self.versions[wrt.key]=wrt.version
      if wrt.reqType == "GET":
        if wrt.version[1] >= self.versions[wrt.key][1]:# checking the version numbers before applying a write
          self.d[wrt.key]=wrt.val # updating the values we got from the leader
          self.versions[wrt.key]=wrt.version

  def _create_replication_chain(self) -> Tuple[dict[str, Optional[str]], dict[str, Optional[str]]]:
    """
    Creates a replication chain for the servers.

    Returns:
        next_chain: A dictionary mapping each server to its next server in the chain.
        prev_chain: A dictionary mapping each server to its previous server in the chain.
    """
    # Fixing the random seed for reproducibility
    random.seed(42)

    # Shuffle the list of server names excluding the current server
    shuffled_servers = self.server_names[:]
    shuffled_servers.remove(self._info.name)
    random.shuffle(shuffled_servers)

    # Place the current server (self._info.name) at the start of the chain
    chain = [self._info.name] + shuffled_servers

    # Initialize next_chain and prev_chain
    next_chain = {}
    prev_chain = {}

    # Build the chains
    for i in range(len(chain)):
        current_server = chain[i]
        next_server = chain[i + 1] if i + 1 < len(chain) else None
        prev_server = chain[i - 1] if i - 1 >= 0 else None

        next_chain[current_server] = next_server
        prev_chain[current_server] = prev_server

    return next_chain, prev_chain

  def _get(self, req: KVGetRequest) -> JsonMessage:
    """
    handling the get request from a client or another server (in case of the leader)
    """

    _logger = server_logger.bind(server_name=self._info.name)
    

    if ((self._info.name == self.leader) or (not any(req.key == key for key, _ in self.buffer.keys()))):
      # directly get the value from the key-value store if the server is the leader or we dont have a dirty entry for the key in the buffer
      val = self.d[req.key]
      version_no = self.versions[req.key][1]
    else:
      # if we have a dirty entry for the key in the buffer, we must ask the leader for the latest version

      valMessage = JsonMessage({"type": "GET", "key": req.key})
      response: Optional[JsonMessage] = self._connection_stub.send(from_=self._info.name, to=self.leader, message=valMessage)
      # the leader will send back the value and the version number

      assert response is not None
      if response["status"] == "OK":
        val = response["val"]
        version_no = response["version_no"]
        self.command_queue.put(QueueElement(key=req.key, reqType="GET", version=(self.versions[req.key][0], version_no),  val=val))
        # we update the value of the key using the response from the leader, this is done via adding a write request in the command queue

    _logger.debug(f"Getting {req.key} as {val}")
    return JsonMessage({"status": "OK", "val": val, "version_no": version_no})

  def _set(self, req: KVSetRequest) -> JsonMessage:
    """
    handling the set request from a server or a client
    """
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting {req.key} to {req.val}")
    
    if req.version is None and self._info.name == self.leader: # if we are the leader then we have to assign the version number

      with self.version_locks[req.key]:
        # for this we must acquire a lock for the key, fetch the next possible version number and also increase the same
        version_num = self.next_version[req.key]+1
        self.next_version[req.key]=version_num
    else:
      # other servers just use the version in the request they got
      version_num = req.version
    
    if self._info.name != self.leader: 
      # all servers except the leader write the new value in the buffer
      self.buffer[(req.key, req.request_id)] = (version_num, req.val)

    req.version = version_num
    # changing the version in the request to be forwarded

    if req.next_chain is None: 
      # if the request doesnt have a next and prev chain then the request must have come from the client, thus we set that in the request being forwarded
      req.next_chain, req.prev_chain = self._create_replication_chain()
      
    next_server = req.next_chain[self._info.name]

    if next_server is not None:
      
      temp= self._connection_stub.send(from_=self._info.name, to=next_server,message=req.json_msg) 

      if self._info.name==self.leader: 
        # if there are other servers in the chain and we are the leader, we must apply the write, as we have got response from the later servers, 
        # thus all servers have seen this write
        req.leader_verif = True
        with self.version_locks[req.key]:   
          # we use lock to ensure this write is safe                   
          if req.key not in self.versions or self.versions[req.key][1] < version_num:
            # checking if the write is valid
            assert version_num is not None
            self.d[req.key] = req.val
            self.versions[req.key] = (req.request_id, version_num)

      if (self._info.name!=self.leader) and (req.leader_verif is not None and req.leader_verif):
        self.command_queue.put(QueueElement(key=req.key, reqType="ACK", version=(req.request_id, version_num)))
      return temp

    else:
      # if this is the last server in the chain, we need to generate an ack message
      if self._info.name == self.leader:
        # if we are the leader, we apply the write locally and also set leader_verif in the ack message
        leader_verif = True
        req.leader_verif = True
        assert version_num is not None # have made this assertion to ensure that we are never writing None as the version number on leader server
        
        with self.version_locks[req.key]:
          # using locks on the leader
          if req.key not in self.versions or self.versions[req.key][1] < version_num:
            self.d[req.key] = req.val
            self.versions[req.key] = (req.request_id, version_num)
      else:
        leader_verif = False

      AckMessage = JsonMessage({"type": "ACK", "key": req.key, "ver": version_num, "request_id" : req.request_id, "leader_verif": leader_verif, "prev_chain": req.prev_chain})
      self._connection_stub.send(from_=self._info.name, to=req.prev_chain[self._info.name], message=AckMessage, blocking=False) # non blocking ack
      return JsonMessage({"status": "OK"})
      
  def _ack(self, req: KVSetRequest) -> JsonMessage:
    """
    handling the ack message from other servers
    """
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting the version of {req.key} to {req.version}")

    key = req.key
    request_id = req.request_id
    version = req.version
    leader_verif = req.leader_verif
    
    # if leader_verif:  
      # if the ack is leader verified then we can apply the changes in the key-value store
      # note that leader can never get leader verified ack
      # self.command_queue.put(QueueElement(key=req.key, reqType="ACK", version=(request_id, version)))
      # if version < self.versions[req.key][1]:
        # print("not applied yet")

    if not leader_verif and self._info.name == self.leader:
      # if it is not verified and we are the leader, we must set it as verified
      req.leader_verif = True

    prev_server = req.prev_chain[self._info.name]
    
    # sending it backwards in the chain or generating a message for the client
    if prev_server is not None: 
      return self._connection_stub.send(from_=self._info.name, to=prev_server, message=req.json_msg)
    else:
      return JsonMessage({"status": "OK"})
