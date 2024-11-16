import json
from enum import Enum
from typing import Optional, Final, Tuple, List

from core.logger import server_logger
from core.message import JsonMessage
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
    def next_chain(self) -> dict[str, Optional[ServerInfo]]:
        return {str(k): v for k, v in self._json_message["next_chain"].items()}

    @property
    def prev_chain(self) -> dict[str, Optional[ServerInfo]]:
        return {str(k): v for k, v in self._json_message["prev_chain"].items()}

    @version.setter
    def version(self, ver: int) -> None:
        self._json_message["ver"] = ver

    @property
    def json_msg(self) -> JsonMessage:
        return self._json_message

    def __str__(self) -> str:
        return str(self._json_message)


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
    def request_id(self) -> Optional[int]:
        return self._json_message.get("request_id")

    @property
    def tail_verif(self) -> Optional[int]:
        return self._json_message.get("tail_verif")

    @property
    def prev_chain(self) -> dict[str, Optional[ServerInfo]]:
        return {str(k): v for k, v in self._json_message["prev_chain"].items()}

    @property
    def json_msg(self) -> JsonMessage:
        return self._json_message

    def __str__(self) -> str:
        return str(self._json_message)


class StarServer(Server):
    """Chain replication. GET is only served by tail"""

    def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
                 next: Optional[ServerInfo], prev: Optional[ServerInfo],
                 tail: ServerInfo) -> None:
        super().__init__(info, connection_stub)
        self.next: Final[Optional[str]] = None if next is None else str(next)
        self.prev: Final[Optional[str]] = None if prev is None else str(prev)
        self.tail: Final[str] = str(tail)
        self.d: dict[str, str] = {}  # Key-Value store
        self.versions: dict[str, Tuple[str, Optional[int]]] = {}  # Tracks versions
        self.versionState: dict[str, str] = {}  # Tracks if a version is clean or dirty

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

    def _get(self, req: KVGetRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)

        if req.key not in self.d:
            return JsonMessage({"status": "OK", "val": 0})

        if self.versionState[req.key] == 'C':
            val = self.d[req.key]
            version_no = self.versions[req.key][1]
        else:
            valMessage = JsonMessage({"type": "GET", "key": req.key})
            response: Optional[JsonMessage] = self._connection_stub.send(
                from_=self._info.name, to=self.tail, message=valMessage)
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
            version_num = self.versions[req.key] + 1
        else:
            version_num = req.version

        self.d[req.key] = req.val
        self.versionState[req.key] = 'D'
        self.versions[req.key] = (req.request_id, version_num)
        req.version = version_num

        next_server = req.next_chain[str(self._info)]
        if next_server is not None:
            return self._connection_stub.send(
                from_=self._info.name, to=str(next_server), message=req.json_msg)
        else:
            if self._info.name == self.tail:
                self.versionState[req.key] = 'C'
            AckMessage = JsonMessage({
                "type": "ACK", "key": req.key, "ver": version_num,
                "request_id": req.request_id, "tail_verif": self._info.name == self.tail,
                "prev_chain": req.prev_chain
            })
            self._connection_stub.send(
                from_=self._info.name, to=str(req.prev_chain[str(self._info)]),
                message=AckMessage, blocking=False)
            return JsonMessage({"status": "OK"})

    def _ack(self, req: KVAckRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.debug(f"Setting the version of {req.key} to {req.version}")

        if self.versions[req.key][0] == req.request_id:
            if req.tail_verif:
                self.versions[req.key] = (req.request_id, req.version)
                self.versionState[req.key] = 'C'

        prev_server = req.prev_chain[str(self._info)]
        if prev_server is not None:
            return self._connection_stub.send(
                from_=self._info.name, to=str(prev_server), message=req.json_msg)
        else:
            return JsonMessage({"status": "OK"})