import random
from typing import Optional, Final

from star.star_server import StarServer
from core.cluster import ClusterManager
from core.message import JsonMessage, JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server

START_PORT: Final[int] = 8000
POOL_SZ = 32

class StarClient():
  def __init__(self, infos: list[ServerInfo], client_id: int):
    self.conns: list[TcpClient] = []
    for info in infos:
      conn = TcpClient(info)
      self.conns.append(conn)
    self.id=client_id
    self.request_no=1

  def set(self, key: str, val: str, server_number: Optional[int] = None) -> bool:
    server_number = server_number if server_number is not None else self.get_server()
    request_id = "client_no" + str(self.id) + "req" + str(self.request_no)
    self.request_no += 1
    # print("SET REQUEST FROM CLIENT",self.id,"FOR", key, "TO VAL:", val, "TO SERVER:", server_number,"WITH REQUEST ID", request_id)
    response: Optional[JsonMessage] = self.conns[server_number].send(JsonMessage({"type": "SET", "key": key, "val": val, "c_id": self.id, "request_id": request_id}))
    # print("SET REQUEST FROM CLIENT",self.id,"FOR", key, "RESPONSE:", response,"WITH REQUEST ID", request_id)
    assert response is not None
    return response["status"] == "OK"

  # need to implement this
  def get(self, key: str, server_number: Optional[int] = None) -> tuple[bool, Optional[str]]:
    server_number = server_number if server_number is not None else self.get_server()
    server=self.conns[server_number]
    # print("GET REQUEST FROM CLIENT",self.id,"FOR", key, "TO SERVER:", server_number)
    response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
    # print("GET REQUEST FROM CLIENT",self.id,"FOR", key, "RESPONSE:", response)
    assert response is not None
    if response["status"] == "OK":
      return True, response["val"]
    return False, response["status"]

  def get_server(self) -> int:
    # Random strategy
    serverNumber = random.randint(0, len(self.conns) - 1)
    return serverNumber

class StarCluster(ClusterManager):
  def __init__(self) -> None:
    self.a = ServerInfo("a", "localhost", START_PORT)
    self.b = ServerInfo("b", "localhost", START_PORT+1)
    self.c = ServerInfo("c", "localhost", START_PORT+2)
    self.d = ServerInfo("d", "localhost", START_PORT+3)
    self.e = ServerInfo("e", "localhost", START_PORT+4)

    self.prev: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: None,
      self.b: self.a,
      self.c: self.b,
      self.d: self.c,
      self.e: self.d,
    }
    self.next: dict[ServerInfo, Optional[ServerInfo]] = {
      self.a: self.b,
      self.b: self.c,
      self.c: self.d,
      self.d: self.e,
      self.e: None,
    }

    self.prev_chain: list[dict[ServerInfo, Optional[ServerInfo]]] = [
      {self.a:None, self.b:self.c, self.c:self.d, self.d:self.e, self.e:self.a},  
      {self.a:self.b, self.b:None, self.c:self.d, self.d:self.e, self.e:self.a},
      {self.a:self.c, self.b:self.d, self.c:None, self.d:self.a, self.e:self.b},
      {self.a:self.c, self.b:self.d, self.c:self.e, self.d:None, self.e:self.b},
      {self.a:self.e, self.b:self.a, self.c:self.b, self.d:self.c, self.e:None},
    ]

    self.next_chain: list[dict[ServerInfo, Optional[ServerInfo]]] = [
      {self.a:self.e, self.b:None, self.c:self.b, self.d:self.c, self.e:self.d},    # 1 -> 5 -> 4 -> 3 -> 2
      {self.a:self.e, self.b:self.a, self.c:None, self.d:self.c, self.e:self.d},    # 2 -> 1 -> 5 -> 4 -> 3
      {self.a:self.d, self.b:self.e, self.c:self.a, self.d:self.b, self.e:None},    # 3 -> 1 -> 4 -> 2 -> 5
      {self.a:None, self.b:self.e, self.c:self.a, self.d:self.b, self.e:self.c},    # 4 -> 2 -> 5 -> 3 -> 1
      {self.a:self.b, self.b:self.c, self.c:self.d, self.d:None, self.e:self.a},    # 5 -> 1 -> 2 -> 3 -> 4
    ]

    super().__init__(
      master_name="d",
      topology={
        self.a: {self.b, self.c, self.d, self.e},
        self.b: {self.a, self.c, self.d, self.e},
        self.c: {self.a, self.b, self.d, self.e},
        self.d: {self.a, self.b, self.c, self.e},
        self.e: {self.a, self.b, self.c, self.d},
      },
      sock_pool_size=POOL_SZ,
    )
  
  def conv_chain(self, chain_list:list[dict[ServerInfo, Optional[ServerInfo]]])->list[dict[str, Optional[str]]]:
    return [{key.name: (value.name if value is not None else None) for key, value in chain.items()}
      for chain in chain_list]

  def connect(self, client_id) -> StarClient:
    return StarClient(infos=[self.a, self.b, self.c, self.d, self.e], client_id=client_id)

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    next_chain = self.conv_chain(self.next_chain)[list(self.next.keys()).index(si)]
    prev_chain = self.conv_chain(self.prev_chain)[list(self.prev.keys()).index(si)]
    return StarServer(info=si, connection_stub=connection_stub,
                      next=self.next[si], prev=self.prev[si],
                      next_chain=next_chain, prev_chain=prev_chain, tail=self.d)
