
# TODO: Implement the Server for CRAQ

import json
from enum import Enum
from typing import Optional, Final

from core.logger import server_logger
from core.message import JsonMessage, JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo


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

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def val(self) -> str:
    return self._json_message["val"]

  @property
  def version(self) -> Optional[int]:
    return self._json_message.get("ver")

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
    assert "val" in self._json_message, self._json_message
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
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)


class CraqServer(Server):
  """Chain replication. GET is only served by tail"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
               next: Optional[ServerInfo], prev: Optional[ServerInfo],
               tail: ServerInfo) -> None:
    super().__init__(info, connection_stub)
    self.next: Final[Optional[str]] = None if next is None else next.name
    self.prev: Final[Optional[str]] = prev if prev is None else prev.name
    self.tail: Final[str] = tail.name
    self.d: dict[str, str] = {} # Key-Value store
    self.versions: dict[str,int] = {} # this will maintain the dictionary from the keys to the version numbers 
    self.versionState: dict[str,str] = {} # this will maintain the dictionary from the keys to the latest version state whether clean or dirty

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

    last_version = self.versions[req.key]
    if (self.versionState[req.key]=='C'):
      val = self.d[req.key]
    else:
      # get the value from the tail and define it as val
      valMessage = JsonMessage({"type": "GET", "key": req.key})
      response: Optional[JsonMessage] = self._connection_stub.send(from_=self._info.name, to=self.tail, message=valMessage)
      assert response is not None
      if response["status"] == "OK":
        val = response["val"]
    _logger.debug(f"Getting {req.key} as {val}")
    return JsonMessage({"status": "OK", "val": val})

  def _set(self, req: KVSetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting {req.key} to {req.val}")

    if (req.key in self.versions):
      self.versions[req.key] += 1
    else:
      self.versions[req.key] = 1
    if (self.next is None):
      self.versionState[req.key] = 'C'
    else:
      self.versionState[req.key] = 'D'

    self.d[req.key] = req.val
    if self.next is not None:
      # Send blocking request
      return self._connection_stub.send(from_=self._info.name, to=self.next,message=req.json_msg)
    else:
      # here if you are the tail then send ack message about the successsful write
      AckMessage = JsonMessage({"type": "ACK", "key": req.key, "ver": self.versions[req.key], "val": req.val})
      self._connection_stub.send(from_=self._info.name, to=self.prev, message=AckMessage, blocking=False)
      return JsonMessage({"status": "OK"})
      
  def _ack(self, req: KVSetRequest) -> JsonMessage:
    _logger = server_logger.bind(server_name=self._info.name)
    _logger.debug(f"Setting the version of {req.key} to {req.version}")
    
    if ((self.versions[req.key] == req.version) and (self.d[req.key] == req.val)):
      self.versionState[req.key] = 'C'

    if self.prev is not None:
      return self._connection_stub.send(from_=self._info.name, to=self.prev, message=req.json_msg)
    else:
      return JsonMessage({"status": "OK"})