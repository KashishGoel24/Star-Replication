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
    """
    initializing the star client class
    Attributes-
      infos: list of Server Info which form the cluster
      client_id: the id of the client
    """
    self.conns: list[TcpClient] = [] # converting server info to tcpClient object
    for info in infos:
      conn = TcpClient(info)
      self.conns.append(conn)
    self.id=client_id
    self.request_no=1 # a counter to keep track of the requests sent by the client

  def set(self, key: str, val: str, server_number: Optional[int] = None) -> bool:
    """
    requesting a server to set a value at a key
    """
    server_number = server_number if server_number is not None else self.get_server()

    # forming the request_id, which is the combination of the client id and its requests so far
    request_id = "client_no" + str(self.id) + "req" + str(self.request_no)
    self.request_no += 1

    # sending the set request to a server, with the request id
    response: Optional[JsonMessage] = self.conns[server_number].send(JsonMessage({"type": "SET", "key": key, "val": val, "c_id": self.id, "request_id": request_id}))
    assert response is not None
    return response["status"] == "OK"


  def get(self, key: str, server_number: Optional[int] = None) -> tuple[bool, Optional[str]]:
    """
    requesting a server for the value at a key
    """
    server_number = server_number if server_number is not None else self.get_server()
    server=self.conns[server_number]
    response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
    assert response is not None
    if response["status"] == "OK":
      return True, response["val"]
    return False, response["status"]

  def _get_server(self) -> int:
    """
    strategy for selecting a server is to choose a server at random
    """
    serverNumber = random.randint(0, len(self.conns) - 1)
    return serverNumber

class StarCluster(ClusterManager):
  def __init__(self) -> None:
    """
    intializing the star cluster class
    """
    self.a = ServerInfo("a", "localhost", START_PORT)
    self.b = ServerInfo("b", "localhost", START_PORT+1)
    self.c = ServerInfo("c", "localhost", START_PORT+2)
    self.d = ServerInfo("d", "localhost", START_PORT+3)
    self.e = ServerInfo("e", "localhost", START_PORT+4)

    # the chains which will be followed if a particular server gets the set request from the client
    self.next_chain: dict[ServerInfo, dict[ServerInfo, Optional[ServerInfo]]] = {
      self.a:{self.a:self.e, self.b:None, self.c:self.b, self.d:self.c, self.e:self.d},    # 1 -> 5 -> 4 -> 3 -> 2
      self.b:{self.a:self.e, self.b:self.a, self.c:None, self.d:self.c, self.e:self.d},    # 2 -> 1 -> 5 -> 4 -> 3
      self.c:{self.a:self.d, self.b:self.e, self.c:self.a, self.d:self.b, self.e:None},    # 3 -> 1 -> 4 -> 2 -> 5
      self.d:{self.a:None, self.b:self.e, self.c:self.a, self.d:self.b, self.e:self.c},    # 4 -> 2 -> 5 -> 3 -> 1
      self.e:{self.a:self.b, self.b:self.c, self.c:self.d, self.d:None, self.e:self.a},    # 5 -> 1 -> 2 -> 3 -> 4
    }

    # the chains in reverse
    self.prev_chain: dict[ServerInfo, dict[ServerInfo, Optional[ServerInfo]]] = {
      self.a:{self.a:None, self.b:self.c, self.c:self.d, self.d:self.e, self.e:self.a},  
      self.b:{self.a:self.b, self.b:None, self.c:self.d, self.d:self.e, self.e:self.a},
      self.c:{self.a:self.c, self.b:self.d, self.c:None, self.d:self.a, self.e:self.b},
      self.d:{self.a:self.c, self.b:self.d, self.c:self.e, self.d:None, self.e:self.b},
      self.e:{self.a:self.e, self.b:self.a, self.c:self.b, self.d:self.c, self.e:None},
    }

    #fully connected topology
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
  
  def conv_chain(self, chain:dict[ServerInfo, Optional[ServerInfo]])->dict[str, Optional[str]]:
    """
    function to convert servers info to server name in the individual chain
    """
    return {key.name: (value.name if value is not None else None) for key, value in chain.items()}
      

  def connect(self, client_id) -> StarClient:
    """
    returns a client object with the given client id
    """
    return StarClient(infos=[self.a, self.b, self.c, self.d, self.e], client_id=client_id)

  def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
    """
    creates a server with the given server info
    """
    # uses the converted forward and backward chains to be passed to the server class
    next_chain = self.conv_chain(self.next_chain[si])
    prev_chain = self.conv_chain(self.prev_chain[si])
    return StarServer(info=si, connection_stub=connection_stub,
                      next_chain=next_chain, prev_chain=prev_chain, leader=self.d)
