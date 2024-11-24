

import json
from enum import Enum
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
  def tail_verif(self) -> Optional[int]:
    return self._json_message.get("tail_verif")
  
  @property
  def prev_chain(self) -> dict[ServerInfo, Optional[ServerInfo]]:
    return self._json_message["prev_chain"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)

  @tail_verif.setter
  def tail_verif(self, ans: bool) -> None:
    self._json_message['tail_verif'] = ans


class StarServer(Server):
  """Chain replication. GET is only served by tail"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
               next: Optional[ServerInfo], prev: Optional[ServerInfo],
               next_chain: dict[str, Optional[str]],
               prev_chain: dict[str, Optional[str]],
               tail: ServerInfo) -> None:
    super().__init__(info, connection_stub)
    self.next: Final[Optional[str]] = None if next is None else next.name
    self.prev: Final[Optional[str]] = prev if prev is None else prev.name
    self.tail: Final[str] = tail.name
    self.d: dict[str, int] = defaultdict(lambda: (0)) # Key-Value store
    self.versions: defaultdict[str, Tuple[str, Optional[int]]] = defaultdict(lambda: ('0', 0))
    self.versionState: defaultdict[str, str] = defaultdict(lambda: ('C'))# this will maintain the dictionary from the keys to the latest version state whether clean or dirty
    self.buffer: dict[Tuple[str, str], Tuple[int, str, int]] = {} # this is mapping (key, request id) to (version number, version state, value) 
    self.next_version: dict[str, int] = defaultdict(lambda: (0))  # this will just keep track of the number of set requests that are pending 
    self.version_locks: dict[str, Lock] = defaultdict(Lock)  # Per-key locks
    self.next_chain: dict[str, Optional[str]] = next_chain
    self.prev_chain: dict[str, Optional[str]] = prev_chain

  def _process_req(self, msg: JsonMessage) -> JsonMessage:
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
    while True:
      wrt=self.command_queue.get()
      if wrt.reqType == "ACK":
        ele = self.buffer.pop((wrt.key, wrt.version[0]))
        if wrt.version[1] >= self.versions[wrt.key][1]:
          self.d[wrt.key]=ele[2]
          self.versions[wrt.key]=wrt.version
          if wrt.versionState is not None:
            self.versionState[wrt.key]=wrt.versionState
      if wrt.reqType == "GET":
        if wrt.version[1] >= self.versions[wrt.key][1]:
          self.d[wrt.key]=wrt.val
          self.versions[wrt.key]=wrt.version
          if wrt.versionState is not None:
            self.versionState[wrt.key]=wrt.versionState
      # print(f"{wrt.reqType} server name {self._info.name} self.d {self.d} self.versionState {self.versionState} self.versions {self.versions} self.buffer {self.buffer}")

  def _get(self, req: KVGetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    # print(f"IN GET REUQEST server name {self._info.name} self.d {self.d} self.versionState {self.versionState} self.versions {self.versions} self.buffer {self.buffer}")

    if ((self._info.name == self.tail) or (not any(req.key == key for key, _ in self.buffer.keys()))):
      val = self.d[req.key]
      version_no = self.versions[req.key][1]
    else:
      # get the value from the tail and define it as val
      valMessage = JsonMessage({"type": "GET", "key": req.key})
      response: Optional[JsonMessage] = self._connection_stub.send(from_=self._info.name, to=self.tail, message=valMessage)
      assert response is not None
      if response["status"] == "OK":
        val = response["val"]
        version_no = response["version_no"]
        self.command_queue.put(QueueElement(key=req.key, reqType="GET", val=val, version=(self.versions[req.key][0], version_no), versionState='C'))

    _logger.debug(f"Getting {req.key} as {val}")
    return JsonMessage({"status": "OK", "val": val, "version_no": version_no})

  def _set(self, req: KVSetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting {req.key} to {req.val}")
    # assigning the version number here
    if req.version is None and self._info.name == self.tail:
      with self.version_locks[req.key]:
        version_num = self.next_version[req.key]+1
        self.next_version[req.key]=version_num
    else:
      version_num = req.version
    prev_version_num=self.versions[req.key][1]

    if self._info.name != self.tail: 
      self.buffer[(req.key, req.request_id)] = (version_num, 'D', req.val)

    req.version = version_num

    if req.next_chain is None:
      req.next_chain = self.next_chain 
      req.prev_chain = self.prev_chain
      
    next_server = req.next_chain[self._info.name]

    if next_server is not None:
      temp= self._connection_stub.send(from_=self._info.name, to=next_server,message=req.json_msg) # this will be status: ok
      if self._info.name==self.tail: 
        with self.version_locks[req.key]:                      
          if req.key not in self.versions or self.versions[req.key][1] < version_num:
            assert version_num is not None
            # self.command_queue.put(QueueElement(key=req.key, reqType="SET", val=req.val, version=(req.request_id, version_num)))
            self.versionState[req.key] = 'C'
            self.d[req.key] = req.val
            self.versions[req.key] = (req.request_id, version_num)
      return temp

    else:
      if self._info.name == self.tail:
        tail_verif = True
        assert version_num is not None      # have made this assertion to ensure that we are never writing None as the version number on tail server
        with self.version_locks[req.key]:
          if req.key not in self.versions or self.versions[req.key][1] < version_num:
            self.versionState[req.key] = 'C'
            self.d[req.key] = req.val
            self.versions[req.key] = (req.request_id, version_num)
      else:
        tail_verif = False

      AckMessage = JsonMessage({"type": "ACK", "key": req.key, "ver": version_num, "request_id" : req.request_id, "tail_verif": tail_verif, "prev_chain": req.prev_chain})
      self._connection_stub.send(from_=self._info.name, to=req.prev_chain[self._info.name], message=AckMessage, blocking=False) # non blocking ack
      return JsonMessage({"status": "OK"})
      
  def _ack(self, req: KVSetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting the version of {req.key} to {req.version}")

    key = req.key
    request_id = req.request_id
    version = req.version
    tail_verif = req.tail_verif
    # print(f"Server name: {self._info.name} ACK")
    prev_version_num=self.versions[req.key][1]
    if tail_verif:  
      self.command_queue.put(QueueElement(key=req.key, reqType="ACK", version=(request_id, version), versionState='C'))

    if not tail_verif and self._info.name == self.tail:
      req.tail_verif = True

    prev_server = req.prev_chain[self._info.name]
    # print(f"JUST BEFORE SENDING ACKS {self._info.name}")
    if prev_server is not None:
      return self._connection_stub.send(from_=self._info.name, to=prev_server, message=req.json_msg)
    else:
      return JsonMessage({"status": "OK"})
