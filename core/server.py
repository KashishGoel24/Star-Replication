from __future__ import annotations

import abc
import socket
import threading
import queue
from dataclasses import dataclass
from multiprocessing import Process
from time import sleep
from typing import TYPE_CHECKING, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

if TYPE_CHECKING:
  from network import ConnectionStub

from core.logger import server_logger
from core.message import JsonMessage
from core.socket_helpers import STATUS_CODE, recv_message

MAX_WORKERS = 32

@dataclass
class ServerInfo:
  name: str
  host: str
  port: int

  def __hash__(self) -> int:
    return hash(f"{self.name} {self.host}:{self.port}")

  def __str__(self) -> str:
    return f"Name={self.name},Address={self.host}:{self.port},"

class QueueElement:
  key: str
  reqType: str
  val: Optional[str]
  version: Optional[Tuple[str, Optional[int]]]
  versionState: Optional[str]

  def __init__(self, key: str,reqType: str, val: Optional[str]=None, version: Optional[Tuple[str, Optional[int]]]=None, versionState: Optional[str]=None):
    self.key=key
    self.reqType = reqType
    self.val=val
    self.version=version
    self.versionState=versionState


class Server(Process):
  """This class represents the CRAQ Server"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub) -> None:
    super(Server, self).__init__()
    self._info = info
    self._connection_stub = connection_stub

  def handle_client(self, client_sock: socket.socket, addr: socket.AddressInfo):
    _logger = server_logger.bind(server_name=self._info.name)
    try:
      while True:
        _logger.debug(f"Connected with {addr}")
        err_code, request = recv_message(client_sock)
        # print("recvd the request from", request)
        if request is None:
          _logger.critical(f"{STATUS_CODE[err_code]}")
          sr = JsonMessage(msg={"error_msg": STATUS_CODE[err_code], "error_code": err_code})
          # print("REQUEST IS NONE IN HANDLE CLIENT")
        else:
          _logger.debug(f"Received message from {addr}: {request}")
          # print(f"PUTTING REQ {request} BY SERVER {self._info.name}")
          sr = self._process_req(request) # we send it to process the request, but here we put things in the queue
          # here will send the request and the client socket as a tuple and once the server processes the request it will respond to it
          # req_q.put((request, client_sock))
        if sr is not None:
          _logger.debug(f"Sending message to {addr}: {sr}")
          client_sock.sendall(sr.serialize())
    except Exception as e:
      _logger.exception(e)
    finally:
      _logger.debug(f"Something went wrong! Closing the socket")
      client_sock.close()

  def run(self) -> None:
    # now in this function we first initialise a command queue
    # we still have the handle client thread
    # however now that thread will put the commands into the queue that we give as its argument
    # another thread will take out the requests from the queue and serve them accordingly
    self.command_queue = queue.Queue()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((self._info.host, self._info.port))
    sock.listen()

    sleep(1)    # Let all servers start listening
    self._connection_stub.initalize_connections()

    _logger = server_logger.bind(server_name=self._info.name)

    _logger.info(f"Listening on {self._info.host}:{self._info.port}")

    # this will start the command queue handler thread that will extract the messages from the queue and keep serving them
    # cmd_thread = CmdHandler(state, req_q)
    # correct the name thing below
    cmd_thread = threading.Thread(target=self._cmd_thread,
          name="CmdHandlerThread")
    cmd_thread.start()

    client_handlers: list[threading.Thread] = []
    try:
      while True:
        client_sock, addr = sock.accept()
        client_handler = threading.Thread(target=self.handle_client,
                                          args=(client_sock, addr), name=f"listen#{addr[1]}")
        client_handler.daemon = True
        client_handler.start()
        client_handlers.append(client_handler)
    finally:
      sock.close()
      for client_handler in client_handlers:
        client_handler.join()
      # check if joining the cmd handler thread here is correct
      cmd_thread.join()

  @abc.abstractmethod
  def _process_req(self, msg: JsonMessage) -> Optional[JsonMessage]:
    raise NotImplementedError
  
  @abc.abstractmethod
  def _cmd_thread(self) -> None:
    raise NotImplementedError

  def __str__(self) -> str:
    return str(self._info)
