import random
from typing import Optional, Final

from star.star_server import StarServer
from core.cluster import ClusterManager
from core.message import JsonMessage, JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server

START_PORT: Final[int] = 9900
# START_PORT: Final[int] = 8700
POOL_SZ = 32

class StarClient():
  # TODO: Implement this class
  # pass

  # this should be the same as that for crClient
  def __init__(self, infos: list[ServerInfo], client_id: int, next_chains: list[dict[ServerInfo, Optional[ServerInfo]]], prev_chains:list[dict[ServerInfo, Optional[ServerInfo]]] ):
    self.conns: list[TcpClient] = []
    for info in infos:
      conn = TcpClient(info)
      self.conns.append(conn)
    self.current = 0
    self.id=client_id
    self.next_chains=next_chains
    self.prev_chains=prev_chains
    self.request_no=1

  # this too should be the same as that of crClient because the set requet is still processed only by the head server
  def set(self, key: str, val: str) -> bool:
    server_number=self._get_server()
    request_id = "client_no" + str(self.id) + "req" + str(self.request_no)
    self.request_no += 1
    response: Optional[JsonMessage] = self.conns[server_number].send(JsonMessage({"type": "SET", "key": key, "val": val, "c_id": self.id, "next_chain": self.next_chains[server_number], "prev_chain": self.prev_chains[server_number], "request_id": request_id}))
    assert response is not None
    return response["status"] == "OK"

  # need to implement this
  def get(self, key: str) -> tuple[bool, Optional[str]]:
    server=self.conns[self._get_server()]
    response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
    assert response is not None
    if response["status"] == "OK":
      return True, response["val"]
    return False, response["status"]

  def _get_server(self) -> int:
    # Random strategy
    serverNumber = random.randint(0, len(self.conns) - 1)
    # next in the line
    # serverNumber = self.current
    # self.current = (self.current + 1)%len(self.conns)
    # print("processing the get request for the client", clientno,"using the server",serverNumber)
    return serverNumber


class StarCluster(ClusterManager):
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

    self.prev_chain: list[dict[ServerInfo, Optional[ServerInfo]]] = [
      {self.a:None, self.b:self.a, self.c:self.b, self.d:self.c},
      {self.a:self.b, self.b:None, self.c:self.d, self.d:self.a},
      {self.a:self.d, self.b:self.a, self.c:None, self.d:self.c},
      {self.a:self.b, self.b:self.c, self.c:self.d, self.d:None},
    ]

    self.next_chain: list[dict[ServerInfo, Optional[ServerInfo]]] = [
      {self.a:self.b, self.b:self.c, self.c:self.d, self.d:None},
      {self.a:self.d, self.b:self.a, self.c:None, self.d:self.c},
      {self.a:self.b, self.b:None, self.c:self.d, self.d:self.a},
      {self.a:None, self.b:self.a, self.c:self.b, self.d:self.c},
    ]

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

  def connect(self, client_id) -> StarClient:
    # TODO: Implement this method
    # pass
    return StarClient([self.a, self.b, self.c, self.d])

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    # TODO: Implement this method
    # pass
    return StarServer(info=si, connection_stub=connection_stub, tail=self.d)
