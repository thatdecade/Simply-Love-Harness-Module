"""
itgmani_harness.py

Library-like harness for controlling ITGmania over a local WebSocket connection.

- Runs a WebSocket server that ITGmania connects to
- Sends binary command packets and receives JSON responses
- Optional wire debug logging to trace every message and packet

Dependencies:
- websockets
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import websockets


CMD_HELLO = 0x01
CMD_GET_STATUS = 0x10
CMD_GET_GROUPS = 0x11
CMD_GET_SONGS = 0x12
CMD_START_SONG = 0x20
CMD_PAUSE = 0x21
CMD_STOP = 0x22

RSP_HELLO = 0x81
RSP_GET_STATUS = 0x90
RSP_GET_GROUPS = 0x91
RSP_GET_SONGS = 0x92
RSP_START_SONG = 0xA0
RSP_PAUSE = 0xA1
RSP_STOP = 0xA2
RSP_ERROR = 0xFF


COMMAND_NAMES: Dict[int, str] = {
    CMD_HELLO: "CMD_HELLO",
    CMD_GET_STATUS: "CMD_GET_STATUS",
    CMD_GET_GROUPS: "CMD_GET_GROUPS",
    CMD_GET_SONGS: "CMD_GET_SONGS",
    CMD_START_SONG: "CMD_START_SONG",
    CMD_PAUSE: "CMD_PAUSE",
    CMD_STOP: "CMD_STOP",
}

RESPONSE_NAMES: Dict[int, str] = {
    RSP_HELLO: "RSP_HELLO",
    RSP_GET_STATUS: "RSP_GET_STATUS",
    RSP_GET_GROUPS: "RSP_GET_GROUPS",
    RSP_GET_SONGS: "RSP_GET_SONGS",
    RSP_START_SONG: "RSP_START_SONG",
    RSP_PAUSE: "RSP_PAUSE",
    RSP_STOP: "RSP_STOP",
    RSP_ERROR: "RSP_ERROR",
}


class ItgmaniError(Exception):
    pass


class ItgmaniNotConnectedError(ItgmaniError):
    pass


class ItgmaniProtocolError(ItgmaniError):
    pass


class ItgmaniRequestTimeoutError(ItgmaniError):
    pass


def _timestamp() -> str:
    return time.strftime("%H:%M:%S")


def encode_uint16_be(value: int) -> bytes:
    return bytes([(value >> 8) & 0xFF, value & 0xFF])


def encode_nt_string(text: str) -> bytes:
    return text.encode("utf-8") + b"\x00"


def build_packet(command_id: int, payload: bytes = b"") -> bytes:
    size = 1 + len(payload)
    return encode_uint16_be(size) + bytes([command_id]) + payload


def parse_packets(buffer: bytearray) -> List[Tuple[int, bytes]]:
    packets: List[Tuple[int, bytes]] = []
    while True:
        if len(buffer) < 3:
            break

        size = (buffer[0] << 8) | buffer[1]
        total_size = 2 + size
        if len(buffer) < total_size:
            break

        command_id = buffer[2]
        payload = bytes(buffer[3:total_size]) if size > 1 else b""
        del buffer[:total_size]
        packets.append((command_id, payload))

    return packets


def payload_to_json(payload: bytes) -> Any:
    payload = payload.rstrip(b"\x00")
    text = payload.decode("utf-8", errors="replace")
    return json.loads(text)


def _hex_id(command_id: int) -> str:
    return f"0x{command_id:02X}"


def _command_name_for_send(command_id: int) -> str:
    return COMMAND_NAMES.get(command_id, "UNKNOWN")


def _response_name_for_receive(response_id: int) -> str:
    return RESPONSE_NAMES.get(response_id, "UNKNOWN")


def _safe_payload_preview(payload: bytes, max_chars: int = 220) -> str:
    trimmed_payload = payload.rstrip(b"\x00")
    text = trimmed_payload.decode("utf-8", errors="replace")
    if len(text) > max_chars:
        return text[:max_chars] + "...(truncated)"
    return text


@dataclass(frozen=True)
class ItgmaniTextEvent:
    raw: str
    kind: str
    value: str


TextEventHandler = Callable[[ItgmaniTextEvent], Awaitable[None]]
ConnectionHandler = Callable[[], Awaitable[None]]


class ItgmaniWireLogger:
    def __init__(self, enabled: bool, logger: logging.Logger) -> None:
        self.enabled = enabled
        self.logger = logger

    def log_send(self, command_id: int, expected_response_id: int, payload_size: int) -> None:
        if not self.enabled:
            return
        command_name = _command_name_for_send(command_id)
        response_name = _response_name_for_receive(expected_response_id)
        self.logger.info(
            "%s SEND %s %s expecting %s %s payload_bytes=%d",
            _timestamp(),
            _hex_id(command_id),
            command_name,
            _hex_id(expected_response_id),
            response_name,
            payload_size,
        )

    def log_text(self, message: str) -> None:
        if not self.enabled:
            return
        self.logger.info("%s INFO %s", _timestamp(), message)

    def log_binary_frame(self, frame_size: int) -> None:
        if not self.enabled:
            return
        self.logger.info("%s INFO BINARY_FRAME bytes=%d", _timestamp(), frame_size)

    def log_packet_received(self, response_id: int, payload_size: int) -> None:
        if not self.enabled:
            return
        response_name = _response_name_for_receive(response_id)
        self.logger.info(
            "%s RECV %s %s payload_bytes=%d",
            _timestamp(),
            _hex_id(response_id),
            response_name,
            payload_size,
        )

    def log_oversize_packet(self, response_id: int, declared_total: int, actual_total: int) -> None:
        if not self.enabled:
            return
        response_name = _response_name_for_receive(response_id)
        self.logger.info(
            "%s WARN OVERSIZE_PACKET %s %s declared_total=%d actual_total=%d using_full_frame_payload=1",
            _timestamp(),
            _hex_id(response_id),
            response_name,
            declared_total,
            actual_total,
        )

    def log_json_decode_error(self, response_id: int, payload: bytes, exception: Exception) -> None:
        if not self.enabled:
            return
        response_name = _response_name_for_receive(response_id)
        preview = _safe_payload_preview(payload)
        self.logger.info(
            "%s WARN JSON_DECODE_ERROR for %s %s: %s preview=%s",
            _timestamp(),
            _hex_id(response_id),
            response_name,
            str(exception),
            preview,
        )


class ItgmaniSession:
    """
    Holds state for a single connected ITGmania websocket.
    """

    def __init__(self, logger: Optional[logging.Logger] = None, wire_debug: bool = False) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._wire_logger = ItgmaniWireLogger(enabled=wire_debug, logger=self._logger)

        self._websocket: Optional[Any] = None
        self._rx_buffer = bytearray()

        self.connected_event = asyncio.Event()
        self.ready_event = asyncio.Event()

        self._response_queues: Dict[int, asyncio.Queue[bytes]] = {}
        self._error_queue: asyncio.Queue[bytes] = asyncio.Queue()

        self._send_lock = asyncio.Lock()

        self._text_event_handler: Optional[TextEventHandler] = None
        self._disconnect_handler: Optional[ConnectionHandler] = None

        self.last_screen: str = ""
        self.last_heartbeat_wall_time: float = 0.0

        self._oversize_capable_response_ids = {RSP_GET_SONGS}

    @property
    def is_connected(self) -> bool:
        return self.connected_event.is_set() and self._websocket is not None

    def set_text_event_handler(self, handler: Optional[TextEventHandler]) -> None:
        self._text_event_handler = handler

    def set_disconnect_handler(self, handler: Optional[ConnectionHandler]) -> None:
        self._disconnect_handler = handler

    def attach(self, websocket: Any) -> None:
        self._websocket = websocket
        self._rx_buffer = bytearray()
        self.connected_event.set()
        self.ready_event.clear()
        self._response_queues = {}
        self._error_queue = asyncio.Queue()
        self.last_screen = ""
        self.last_heartbeat_wall_time = 0.0
        self._logger.info("%s ITGmania connected", _timestamp())

    def detach(self) -> None:
        self._websocket = None
        self.connected_event.clear()
        self.ready_event.clear()
        self._logger.info("%s ITGmania disconnected", _timestamp())

    def _get_response_queue(self, response_id: int) -> asyncio.Queue[bytes]:
        queue = self._response_queues.get(response_id)
        if queue is None:
            queue = asyncio.Queue()
            self._response_queues[response_id] = queue
        return queue

    async def _emit_text_event(self, text_event: ItgmaniTextEvent) -> None:
        handler = self._text_event_handler
        if handler is None:
            return
        try:
            await handler(text_event)
        except Exception:
            self._logger.exception("Text event handler raised")

    async def _emit_disconnect(self) -> None:
        handler = self._disconnect_handler
        if handler is None:
            return
        try:
            await handler()
        except Exception:
            self._logger.exception("Disconnect handler raised")

    @staticmethod
    def _split_text_message(message: str) -> Tuple[str, str]:
        if "|" in message:
            prefix, value = message.split("|", 1)
            return prefix.strip(), value
        return message.strip(), ""

    async def _handle_text_message(self, message: str) -> None:
        self._wire_logger.log_text(message)

        prefix, value = self._split_text_message(message)

        if prefix.upper().startswith("HEARTBEAT"):
            self.ready_event.set()
            self.last_heartbeat_wall_time = time.time()
            await self._emit_text_event(ItgmaniTextEvent(raw=message, kind="heartbeat", value=value))
            return

        if prefix.upper().startswith("SCREEN"):
            screen_name = value.strip()
            if screen_name:
                self.last_screen = screen_name
            await self._emit_text_event(ItgmaniTextEvent(raw=message, kind="screen", value=screen_name))
            return

        await self._emit_text_event(ItgmaniTextEvent(raw=message, kind="text", value=message))

    async def _dispatch_packet(self, response_id: int, payload: bytes) -> None:
        self._wire_logger.log_packet_received(response_id, len(payload))
        if response_id == RSP_ERROR:
            await self._error_queue.put(payload)
        else:
            await self._get_response_queue(response_id).put(payload)

    async def _handle_binary_message(self, message_bytes: bytes) -> None:
        self._wire_logger.log_binary_frame(len(message_bytes))

        if len(message_bytes) < 3:
            self._rx_buffer.extend(message_bytes)
            for response_id, payload in parse_packets(self._rx_buffer):
                await self._dispatch_packet(response_id, payload)
            return

        declared_size = (message_bytes[0] << 8) | message_bytes[1]
        declared_total = 2 + declared_size
        response_id = message_bytes[2]
        actual_total = len(message_bytes)

        if declared_total == actual_total:
            payload = message_bytes[3:] if declared_size > 1 else b""
            await self._dispatch_packet(response_id, payload)
            return

        difference = actual_total - declared_total
        is_wrap_pattern = difference > 0 and (difference % 65536 == 0)

        if response_id in self._oversize_capable_response_ids and is_wrap_pattern and actual_total >= 3:
            self._wire_logger.log_oversize_packet(response_id, declared_total=declared_total, actual_total=actual_total)
            payload = message_bytes[3:]
            await self._dispatch_packet(response_id, payload)
            return

        self._rx_buffer.extend(message_bytes)
        for parsed_response_id, payload in parse_packets(self._rx_buffer):
            await self._dispatch_packet(parsed_response_id, payload)

    async def recv_loop(self) -> None:
        websocket = self._websocket
        if websocket is None:
            return

        try:
            async for message in websocket:
                if not self.ready_event.is_set():
                    self.ready_event.set()

                if isinstance(message, str):
                    await self._handle_text_message(message)
                    continue

                if isinstance(message, (bytes, bytearray)):
                    await self._handle_binary_message(bytes(message))
                    continue

        except Exception as exception:
            self._logger.info("recv_loop ended: %r", exception)
        finally:
            self.detach()
            await self._emit_disconnect()

    async def request(
        self,
        command_id: int,
        expected_response_id: int,
        payload: bytes = b"",
        timeout_seconds: float = 10.0,
    ) -> Any:
        if self._websocket is None:
            raise ItgmaniNotConnectedError("Not connected")

        async with self._send_lock:
            self._wire_logger.log_send(command_id, expected_response_id, len(payload))
            await self._websocket.send(build_packet(command_id, payload))

            expected_queue = self._get_response_queue(expected_response_id)
            end_time = time.perf_counter() + timeout_seconds

            while time.perf_counter() < end_time:
                remaining = max(0.1, end_time - time.perf_counter())

                expected_task = asyncio.create_task(expected_queue.get())
                error_task = asyncio.create_task(self._error_queue.get())

                done, pending = await asyncio.wait(
                    {expected_task, error_task},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for pending_task in pending:
                    pending_task.cancel()

                if not done:
                    continue

                finished_task = next(iter(done))
                payload_bytes = finished_task.result()

                if finished_task is error_task:
                    try:
                        error_obj = payload_to_json(payload_bytes)
                    except Exception as exception:
                        self._wire_logger.log_json_decode_error(RSP_ERROR, payload_bytes, exception)
                        raise ItgmaniProtocolError("ITG RSP_ERROR contained invalid JSON") from exception
                    raise ItgmaniProtocolError(f"ITG RSP_ERROR: {error_obj}")

                try:
                    return payload_to_json(payload_bytes)
                except Exception as exception:
                    self._wire_logger.log_json_decode_error(expected_response_id, payload_bytes, exception)
                    raise ItgmaniProtocolError(
                        f"Invalid JSON payload for response {_hex_id(expected_response_id)} {_response_name_for_receive(expected_response_id)}"
                    ) from exception

            raise ItgmaniRequestTimeoutError(
                f"Timed out waiting for response {_hex_id(expected_response_id)} {_response_name_for_receive(expected_response_id)}"
            )


async def hello_with_retry(session: ItgmaniSession, max_attempts: int = 3) -> Any:
    last_exception: Optional[Exception] = None
    timeouts = [10.0, 20.0, 30.0]

    for attempt_index in range(max_attempts):
        timeout_seconds = timeouts[min(attempt_index, len(timeouts) - 1)]
        try:
            return await session.request(CMD_HELLO, RSP_HELLO, timeout_seconds=timeout_seconds)
        except Exception as exception:
            last_exception = exception
            await asyncio.sleep(1.0)

    raise ItgmaniProtocolError(f"HELLO failed after {max_attempts} attempts: {last_exception}")


async def fetch_songs(session: ItgmaniSession, max_count: int = 200, group_filter: str = "") -> List[dict]:
    payload = encode_uint16_be(max_count) + encode_nt_string(group_filter)
    response_object = await session.request(CMD_GET_SONGS, RSP_GET_SONGS, payload=payload, timeout_seconds=30.0)
    songs = response_object.get("songs", [])
    if not isinstance(songs, list):
        return []
    usable: List[dict] = []
    for song in songs:
        if isinstance(song, dict):
            usable.append(song)
    return usable


class ItgmaniClient:
    def __init__(self, session: ItgmaniSession) -> None:
        self._session = session

    @property
    def session(self) -> ItgmaniSession:
        return self._session

    async def hello(self, max_attempts: int = 3) -> Any:
        return await hello_with_retry(self._session, max_attempts=max_attempts)

    async def get_status(self, timeout_seconds: float = 10.0) -> Any:
        return await self._session.request(CMD_GET_STATUS, RSP_GET_STATUS, timeout_seconds=timeout_seconds)

    async def get_groups(self, timeout_seconds: float = 10.0) -> Any:
        return await self._session.request(CMD_GET_GROUPS, RSP_GET_GROUPS, timeout_seconds=timeout_seconds)

    async def get_songs(self, max_count: int = 200, group_filter: str = "", timeout_seconds: float = 30.0) -> Any:
        payload = encode_uint16_be(max_count) + encode_nt_string(group_filter)
        return await self._session.request(CMD_GET_SONGS, RSP_GET_SONGS, payload=payload, timeout_seconds=timeout_seconds)

    async def start_song(self, song_dir: str, difficulty: str, timeout_seconds: float = 15.0) -> Any:
        start_payload = encode_nt_string(song_dir) + encode_nt_string(difficulty)
        return await self._session.request(CMD_START_SONG, RSP_START_SONG, payload=start_payload, timeout_seconds=timeout_seconds)

    async def set_paused(self, paused: bool, timeout_seconds: float = 10.0) -> Any:
        payload = bytes([1 if paused else 0])
        return await self._session.request(CMD_PAUSE, RSP_PAUSE, payload=payload, timeout_seconds=timeout_seconds)

    async def stop(self, timeout_seconds: float = 15.0) -> Any:
        return await self._session.request(CMD_STOP, RSP_STOP, timeout_seconds=timeout_seconds)

    async def fetch_song_list(self, max_count: int = 200, group_filter: str = "") -> List[dict]:
        return await fetch_songs(self._session, max_count=max_count, group_filter=group_filter)


class ItgmaniHarnessServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        logger: Optional[logging.Logger] = None,
        wire_debug: bool = False,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._host = host
        self._port = port

        self.session = ItgmaniSession(logger=self._logger, wire_debug=wire_debug)

        self._websocket_server: Optional[Any] = None
        self._recv_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if self._websocket_server is not None:
            return

        async def handler(websocket: Any) -> None:
            if self.session.is_connected:
                try:
                    await websocket.close(code=1013, reason="Another client is already connected")
                except Exception:
                    pass
                return

            self.session.attach(websocket)
            self._recv_task = asyncio.create_task(self.session.recv_loop())
            await self._recv_task

        self._websocket_server = await websockets.serve(
            handler,
            host=self._host,
            port=self._port,
            ping_interval=None,
        )

        self._logger.info("%s Listening on ws://%s:%d", _timestamp(), self._host, self._port)

    async def stop(self) -> None:
        if self._websocket_server is None:
            return

        try:
            self._websocket_server.close()
            await self._websocket_server.wait_closed()
        finally:
            self._websocket_server = None

        if self._recv_task is not None:
            self._recv_task.cancel()
            self._recv_task = None

        self.session.detach()

    async def __aenter__(self) -> "ItgmaniHarnessServer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()
