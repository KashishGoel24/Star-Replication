import random
from typing import Optional, Final

from star.star_server import StarServer
from core.cluster import ClusterManager
from core.message import JsonMessage, JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server

# START_PORT: Final[int] = 9900
START_PORT: Final[int] = 9000
POOL_SZ = 32

class StarClient():
  # TODO: Implement this class
  # pass

  # this should be the same as that for crClient
  def __init__(self, infos: list[ServerInfo], client_id: int, next_chains: list[dict[str, Optional[str]]], prev_chains:list[dict[str, Optional[str]]] ):
    self.conns: list[TcpClient] = []
    for info in infos:
      conn = TcpClient(info)
      self.conns.append(conn)
    self.id=client_id
    self.next_chains=next_chains
    self.prev_chains=prev_chains
    self.request_no=1
  
  
  def set(self, key: str, val: str) -> bool:
    # server_number=self._get_server
    server_number = 2
    request_id = "client_no" + str(self.id) + "req" + str(self.request_no)
    self.request_no += 1
    # print("sending the request to set the key:", key, "to val:", val, "to server:", server_number)
    response: Optional[JsonMessage] = self.conns[server_number].send(JsonMessage({"type": "SET", "key": key, "val": val, "c_id": self.id, "next_chain": self.next_chains[server_number], "prev_chain": self.prev_chains[server_number], "request_id": request_id}))
    assert response is not None
    return response["status"] == "OK"

  # need to implement this
  def get(self, key: str) -> tuple[bool, Optional[str]]:
    server_number=self._get_server()
    server=self.conns[server_number]
    # print(f"sending the request to get key: {key} to server: {server_number}")
    response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
    assert response is not None
    if response["status"] == "OK":
      return True, response["val"]
    return False, response["status"]

  def _get_server(self) -> int:
    # Random strategy
    serverNumber = random.randint(0, len(self.conns) - 1)
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
        self.a: {self.b, self.d, self.c},
        self.b: {self.a, self.c, self.d},
        self.c: {self.b, self.d, self.a},
        self.d: {self.c, self.a, self.b},
      },
      sock_pool_size=POOL_SZ,
    )
  
  def conv_chain(self, chain_list:list[dict[ServerInfo, Optional[ServerInfo]]])->list[dict[str, Optional[str]]]:
    return [{key.name: (value.name if value is not None else None) for key, value in chain.items()}
      for chain in chain_list]

  def connect(self, client_id) -> StarClient:
    return StarClient(infos=[self.a, self.b, self.c, self.d], client_id=client_id, next_chains=self.conv_chain(self.next_chain), prev_chains=self.conv_chain(self.prev_chain))

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    return StarServer(info=si, connection_stub=connection_stub, next=self.next[si], prev=self.prev[si], tail=self.d)
