import random
from typing import Optional, Final

from star.star_server import StarServer
from core.cluster import ClusterManager
from core.message import JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server

START_PORT: Final[int] = 9900
POOL_SZ = 32


class StarClient:
    def __init__(
        self,
        infos: list[ServerInfo],
        client_id: int,
        next_chains: list[dict[str, Optional[ServerInfo]]],
        prev_chains: list[dict[str, Optional[ServerInfo]]],
    ):
        self.conns: list[TcpClient] = []
        for info in infos:
            conn = TcpClient(info)
            self.conns.append(conn)
        self.current = 0
        self.id = client_id
        self.next_chains = next_chains
        self.prev_chains = prev_chains
        self.request_no = 1

    def set(self, key: str, val: str) -> bool:
        server_number = self._get_server()
        request_id = f"client_no{self.id}req{self.request_no}"
        self.request_no += 1

        # Convert ServerInfo keys to strings using str()
        next_chain = {str(k): v for k, v in self.next_chains[server_number].items()}
        prev_chain = {str(k): v for k, v in self.prev_chains[server_number].items()}

        response: Optional[JsonMessage] = self.conns[server_number].send(
            JsonMessage({
                "type": "SET",
                "key": key,
                "val": val,
                "c_id": self.id,
                "next_chain": next_chain,
                "prev_chain": prev_chain,
                "request_id": request_id
            })
        )
        assert response is not None
        return response["status"] == "OK"

    def get(self, key: str) -> tuple[bool, Optional[str]]:
        server = self.conns[self._get_server()]
        response: Optional[JsonMessage] = server.send(JsonMessage({"type": "GET", "key": key}))
        assert response is not None
        if response["status"] == "OK":
            return True, response["val"]
        return False, response["status"]

    def _get_server(self) -> int:
        server_number = random.randint(0, len(self.conns) - 1)
        return server_number


class StarCluster(ClusterManager):
    def __init__(self) -> None:
        self.a = ServerInfo("a", "localhost", START_PORT)
        self.b = ServerInfo("b", "localhost", START_PORT + 1)
        self.c = ServerInfo("c", "localhost", START_PORT + 2)
        self.d = ServerInfo("d", "localhost", START_PORT + 3)

        # Convert keys to strings using str()
        self.prev: dict[str, Optional[ServerInfo]] = {
            str(self.a): None,
            str(self.b): self.a,
            str(self.c): self.b,
            str(self.d): self.c,
        }
        self.next: dict[str, Optional[ServerInfo]] = {
            str(self.a): self.b,
            str(self.b): self.c,
            str(self.c): self.d,
            str(self.d): None,
        }

        self.prev_chain: list[dict[str, Optional[ServerInfo]]] = [
            {str(self.a): None, str(self.b): self.a, str(self.c): self.b, str(self.d): self.c},
            {str(self.a): self.b, str(self.b): None, str(self.c): self.d, str(self.d): self.a},
            {str(self.a): self.d, str(self.b): self.a, str(self.c): None, str(self.d): self.c},
            {str(self.a): self.b, str(self.b): self.c, str(self.c): self.d, str(self.d): None},
        ]

        self.next_chain: list[dict[str, Optional[ServerInfo]]] = [
            {str(self.a): self.b, str(self.b): self.c, str(self.c): self.d, str(self.d): None},
            {str(self.a): self.d, str(self.b): self.a, str(self.c): None, str(self.d): self.c},
            {str(self.a): self.b, str(self.b): None, str(self.c): self.d, str(self.d): self.a},
            {str(self.a): None, str(self.b): self.a, str(self.c): self.b, str(self.d): self.c},
        ]

        super().__init__(
            master_name="d",
            topology={
                str(self.a): {self.b, self.d},
                str(self.b): {self.a, self.c, self.d},
                str(self.c): {self.b, self.d},
                str(self.d): {self.c},
            },
            sock_pool_size=POOL_SZ,
        )

    def connect(self, client_id) -> StarClient:
        return StarClient(
            infos=[self.a, self.b, self.c, self.d],
            client_id=client_id,
            next_chains=self.next_chain,
            prev_chains=self.prev_chain,
        )

    def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
        return StarServer(
            info=si,
            connection_stub=connection_stub,
            next=self.next[str(si)],
            prev=self.prev[str(si)],
            tail=self.d,
        )
