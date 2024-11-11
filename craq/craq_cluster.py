import random
from typing import Optional, Final

from craq.craq_server import CraqServer
from core.cluster import ClusterManager
from core.message import JsonMessage, JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server

START_PORT: Final[int] = 9900
# START_PORT: Final[int] = 8700
POOL_SZ = 32

class CraqClient():
  # TODO: Implement this class
  # pass

  # this should be the same as that for crClient
  def __init__(self, infos: list[ServerInfo]):
    self.conns: list[TcpClient] = []
    for info in infos:
      conn = TcpClient(info)
      self.conns.append(conn)
    self.current = 0

  # this too should be the same as that of crClient because the set requet is still processed only by the head server
  def set(self, key: str, val: str) -> bool:
    response: Optional[JsonMessage] = self.conns[0].send(JsonMessage({"type": "SET", "key": key, "val": val}))
    assert response is not None
    return response["status"] == "OK"

  # need to implement this
  def get(self, key: str, clientno="0") -> tuple[bool, Optional[str]]:
    response: Optional[JsonMessage] = self._get_server(clientno).send(JsonMessage({"type": "GET", "key": key}))
    assert response is not None
    if response["status"] == "OK":
      return True, response["val"]
    return False, response["status"]

  def _get_server(self, clientno) -> TcpClient:
    # Random strategy
    serverNumber = random.randint(0, len(self.conns) - 1)
    # next in the line
    # serverNumber = self.current
    # self.current = (self.current + 1)%len(self.conns)
    # print("processing the get request for the client", clientno,"using the server",serverNumber)
    return self.conns[serverNumber]


class CraqCluster(ClusterManager):
  def __init__(self) -> None:
    # TODO: Initialize the cluster
    # pass
    self.a = ServerInfo("a", "localhost", START_PORT)
    self.b = ServerInfo("b", "localhost", START_PORT+1)
    self.c = ServerInfo("c", "localhost", START_PORT+2)
    self.d = ServerInfo("d", "localhost", START_PORT+3)

    self.prev: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: None,
      self.b: self.a,
      self.c: self.b,
      self.d: self.c,
    }
    self.next: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: self.b,
      self.b: self.c,
      self.c: self.d,
      self.d: None,
    }

    super().__init__(
      master_name="d",
      topology={
        self.a: {self.b, self.d},
        self.b: {self.a, self.c, self.d},
        self.c: {self.b, self.d},
        self.d: {self.c},
      },
      sock_pool_size=POOL_SZ,
    )

  def connect(self) -> CraqClient:
    # TODO: Implement this method
    # pass
    return CraqClient([self.a, self.b, self.c, self.d])

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    # TODO: Implement this method
    # pass
    return CraqServer(info=si, connection_stub=connection_stub, next=self.next[si], prev=self.prev[si], tail=self.d)
