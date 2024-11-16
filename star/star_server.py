

import json
from enum import Enum
from typing import Optional, Final, Tuple, List

from core.logger import server_logger
from core.message import JsonMessage, JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo
from collections import defaultdict

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
  def next_chain(self) -> dict[ServerInfo, Optional[ServerInfo]]:
    return self._json_message["next_chain"]
  
  @property
  def prev_chain(self) -> dict[ServerInfo, Optional[ServerInfo]]:
    return self._json_message["prev_chain"]

  @version.setter
  def version(self, ver: int) -> None:
    self._json_message['ver'] = ver
  

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)

# type, key, ver, val
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

  @version.setter
  def tail_verif(self, ans: bool) -> None:
    self._json_message['tail_verif'] = ans


class StarServer(Server):
  """Chain replication. GET is only served by tail"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
               next: Optional[ServerInfo], prev: Optional[ServerInfo],
               tail: ServerInfo) -> None:
    super().__init__(info, connection_stub)
    self.next: Final[Optional[str]] = None if next is None else next.name
    self.prev: Final[Optional[str]] = prev if prev is None else prev.name
    self.tail: Final[str] = tail.name
    self.d: dict[str, str] = {} # Key-Value store
    # self.versions: dict[str, Tuple[str, Optional[int]]] = {} # this will maintain the dictionary from the keys to the version numbers 
    self.versions: defaultdict[str, Tuple[str, Optional[int]]] = defaultdict(lambda: ('0', 0))
    self.versionState: defaultdict[str, str] = defaultdict(lambda: ('0'))# this will maintain the dictionary from the keys to the latest version state whether clean or dirty

  def _process_req(self, msg: JsonMessage) -> JsonMessage:
    if msg.get("type") == RequestType.GET.name:
      return self._get(KVGetRequest(msg))
    elif msg.get("type") == RequestType.SET.name:
      return self._set(KVSetRequest(msg))
    # make a request here to process the confirmation acknowledgment from the tail
    # and make the version clean
    elif msg.get("type") == RequestType.ACK.name:
      # here you have received a ack from the tail for the version that has become clean
      # so make the apt changes in your dictionary
      return self._ack(KVAckRequest(msg))
    else:
      server_logger.critical("Invalid message type")
      return JsonMessage({"status": "Unexpected type"})

  def _get(self, req: KVGetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)

    if (req.key not in self.d):
      return JsonMessage({"status": "OK", "val": 0})

    # last_version = self.versions[req.key]
    if (self.versionState[req.key]=='C' or self._info.name==self.tail):
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
        if self.versions[req.key][1] is not None and self.versions[req.key][1] <= version_no:
          self.versions[req.key] = (self.versions[req.key][0], version_no)
          self.d[req.key] = val
          self.versionState[req.key] = 'C'
    _logger.debug(f"Getting {req.key} as {val}")
    return JsonMessage({"status": "OK", "val": val, "version_no": version_no})

  def _set(self, req: KVSetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting {req.key} to {req.val}")

    if req.version is None and self._info.name == self.tail:
      version_num = self.versions[req.key][1] + 1
    else:
      version_num = req.version

    prev_version_num=self.versions[req.key][1]

    if self._info.name != self.tail and (prev_version_num is None or version_num is None or prev_version_num<version_num): # we dont apply to write to the tail till everyone has seen it
      # if we have prev vn none then we apply every write, if we have new vn none then too we apply every write, in case prev vn and vn have a numberical value, we dont apply the new write
      self.d[req.key] = req.val 
      self.versionState[req.key] = 'D'
      self.versions[req.key] = (req.request_id, version_num) # we need to ensure max
    req.version = version_num         # hoping that the request now has the version number defined

    ##### check if we should send blocking or non blocking requests here
    next_server = req.next_chain[self._info.name]

    if next_server is not None:

      temp= self._connection_stub.send(from_=self._info.name, to=next_server,message=req.json_msg) # this will be status: ok

      if self._info.name==self.tail: # we can apply the write here

        # tail_verif = True # we dont need it here? 
        # self.versionState[req.key] = 'C' # we will never have a dirty entry here
        self.d[req.key] = req.val # no need to store the new value in a temp buffer
        self.versions[req.key] = (req.request_id, version_num)

      return temp

    else:

      if self._info.name == self.tail:
        tail_verif = True
        # self.versionState[req.key] = 'C'
        self.d[req.key] = req.val
        self.versions[req.key] = (req.request_id, version_num)

      else:

        tail_verif= False

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

    prev_version_num=self.versions[req.key][1]

    if self.versions[req.key][0] == request_id:
      if tail_verif and (prev_version_num is None or prev_version_num<version): # checking this ensures that we dont clean the version for the servers that come after the tail and get the acks before the tail
        self.versions[req.key] = (request_id, version) # the second check ensures we dont overwrite the latest version just because an ack came late
        self.versionState[req.key] = 'C'

      if not tail_verif and self._info.name == self.tail:
        req.tail_verif = True
        self.versionState[req.key] = 'C'

    prev_server = req.prev_chain[self._info.name]

    if prev_server is not None:
      return self._connection_stub.send(from_=self._info.name, to=prev_server, message=req.json_msg)
    else: # we have reached the start of the chain
      return JsonMessage({"status": "OK"})