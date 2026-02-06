from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, List, Optional

from itgmania_comms import ItgmaniClient, ItgmaniHarnessServer, ItgmaniTextEvent

WIRE_DEBUG_DEFAULT = False


def safe_string(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return ""


def _parse_kv_fields(text: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for part in text.split("|"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        fields[key.strip()] = value.strip()
    return fields


def _screen_from_heartbeat_value(value: str) -> str:
    return _parse_kv_fields(value).get("screen", "").strip()


def _screen_from_screen_value(value: str) -> str:
    # RemoteWsHarness can send: SCREEN|<screen>|uptime_seconds=...
    return value.split("|", 1)[0].strip()


@dataclass(frozen=True)
class UiSong:
    song_dir: str
    title: str
    artist: str
    group: str
    difficulties: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {"song_dir": self.song_dir, "title": self.title, "artist": self.artist, "group": self.group, "difficulties": list(self.difficulties)}


class AsyncioThreadRunner:
    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ready_event = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return

        def thread_target() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._ready_event.set()
            loop.run_forever()

        self._thread = threading.Thread(target=thread_target, name="asyncio-runner", daemon=True)
        self._thread.start()
        self._ready_event.wait()

    def submit(self, coroutine: Any) -> "asyncio.Future[Any]":
        loop = self._loop
        if loop is None:
            raise RuntimeError("Asyncio loop is not running")
        return asyncio.run_coroutine_threadsafe(coroutine, loop)

    def stop(self) -> None:
        loop = self._loop
        if loop is None:
            return
        loop.call_soon_threadsafe(loop.stop)


class ItgmaniaHarnessRunner:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        logger: Optional[logging.Logger] = None,
        wire_debug: bool = WIRE_DEBUG_DEFAULT,
        status_poll_interval_seconds: float = 0.25,
        hello_retry_interval_seconds: float = 1.0,
        songs_autoload_interval_seconds: float = 1.0,
        songs_missing_tip_after_seconds: float = 2.0,
        on_connection_state: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_status: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_songs: Optional[Callable[[List[UiSong]], None]] = None,
        on_log: Optional[Callable[[str], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._wire_debug = wire_debug
        self._logger = logger or logging.getLogger("itgmania")
        self._status_poll_interval_seconds = status_poll_interval_seconds
        self._hello_retry_interval_seconds = hello_retry_interval_seconds
        self._songs_autoload_interval_seconds = songs_autoload_interval_seconds
        self._songs_missing_tip_after_seconds = songs_missing_tip_after_seconds
        self._on_connection_state = on_connection_state
        self._on_status = on_status
        self._on_songs = on_songs
        self._on_log = on_log
        self._on_disconnect = on_disconnect

        self._runner = AsyncioThreadRunner()
        self._runner.start()

        self._server: Optional[ItgmaniHarnessServer] = None
        self._client: Optional[ItgmaniClient] = None
        self._stop_requested = threading.Event()
        self._ready_detected = False
        self._songs_loaded_once = False
        self._songs_refresh_lock: Optional[asyncio.Lock] = None

        self._runner.submit(self._run())

    def stop(self) -> None:
        self._stop_requested.set()
        self._runner.submit(self._shutdown())

    def request_refresh_songs(self) -> None:
        self._runner.submit(self._refresh_songs_once(source_label="manual"))

    def request_start_song(self, song_dir: str, difficulty: str) -> None:
        self._runner.submit(self._start_song(song_dir=song_dir, difficulty=difficulty))

    def request_set_paused(self, paused: bool) -> None:
        self._runner.submit(self._set_paused(paused=paused))

    def request_stop_song(self) -> None:
        self._runner.submit(self._stop_song())

    def _emit_log(self, message: str) -> None:
        self._logger.info(message)
        if self._on_log is not None:
            try:
                self._on_log(message)
            except Exception:
                pass

    def _mark_ready(self) -> None:
        if not self._ready_detected:
            self._ready_detected = True

    def _emit_connection_state(self, connected: bool, ready: bool, screen: str) -> None:
        if self._on_connection_state is None:
            return
        try:
            self._on_connection_state({"connected": bool(connected), "ready": bool(ready), "screen": safe_string(screen)})
        except Exception:
            pass

    def _emit_status(self, response_object: Dict[str, Any]) -> None:
        if self._on_status is None:
            return
        try:
            self._on_status(response_object)
        except Exception:
            pass

    def _emit_songs(self, songs: List[UiSong]) -> None:
        if self._on_songs is None:
            return
        try:
            self._on_songs(songs)
        except Exception:
            pass

    def _emit_disconnect(self) -> None:
        if self._on_disconnect is None:
            return
        try:
            self._on_disconnect()
        except Exception:
            pass

    async def _run(self) -> None:
        self._songs_refresh_lock = asyncio.Lock()
        self._server = ItgmaniHarnessServer(host=self._host, port=self._port, logger=self._logger, wire_debug=self._wire_debug)
        await self._server.start()
        self._client = ItgmaniClient(self._server.session)

        async def on_text_event(text_event: ItgmaniTextEvent) -> None:
            server = self._server
            if server is None:
                return
            if text_event.kind == "heartbeat":
                self._mark_ready()
                screen_name = _screen_from_heartbeat_value(safe_string(text_event.value))
                if screen_name:
                    server.session.last_screen = screen_name
                self._emit_connection_state(True, self._ready_detected, server.session.last_screen)
            if text_event.kind == "screen":
                screen_name = _screen_from_screen_value(safe_string(text_event.value))
                if screen_name:
                    server.session.last_screen = screen_name
                self._emit_connection_state(True, self._ready_detected, server.session.last_screen)

        async def on_disconnect() -> None:
            self._ready_detected = False
            self._songs_loaded_once = False
            self._emit_songs([])
            self._emit_status({})
            self._emit_connection_state(False, False, "")
            self._emit_disconnect()

        self._server.session.set_text_event_handler(on_text_event)
        self._server.session.set_disconnect_handler(on_disconnect)

        while not self._stop_requested.is_set():
            self._ready_detected = False
            self._songs_loaded_once = False
            self._emit_connection_state(False, False, "")
            self._emit_log(f"Listening on ws://{self._host}:{self._port} and waiting for ITGmania to connect")

            while not self._stop_requested.is_set():
                if self._server.session.is_connected:
                    break
                try:
                    await asyncio.wait_for(self._server.session.connected_event.wait(), timeout=0.25)
                    break
                except asyncio.TimeoutError:
                    continue

            if self._stop_requested.is_set():
                break

            self._emit_connection_state(True, False, self._server.session.last_screen)
            self._emit_log("ITGmania connected")

            poll_task = asyncio.create_task(self._poll_status_loop())
            hello_task = asyncio.create_task(self._hello_loop())
            songs_task = asyncio.create_task(self._songs_autoload_loop())

            try:
                while self._server.session.is_connected and not self._stop_requested.is_set():
                    self._emit_connection_state(True, self._ready_detected, self._server.session.last_screen)
                    await asyncio.sleep(self._status_poll_interval_seconds)
            finally:
                for task in [poll_task, hello_task, songs_task]:
                    task.cancel()
                for task in [poll_task, hello_task, songs_task]:
                    try:
                        await task
                    except Exception:
                        pass

            self._emit_log("Connection dropped, waiting for reconnect")

        await self._shutdown()

    async def _shutdown(self) -> None:
        server = self._server
        if server is None:
            return
        try:
            await server.stop()
        except Exception:
            pass

    async def _hello_loop(self) -> None:
        client = self._client
        server = self._server
        if client is None or server is None:
            return
        while server.session.is_connected and not self._stop_requested.is_set():
            if self._ready_detected:
                return
            try:
                response_object = await client.hello(max_attempts=1)
                self._mark_ready()
                self._emit_log(f"HELLO ok: {response_object}")
                return
            except Exception as exception:
                self._emit_log(f"HELLO retry: {exception}")
            await asyncio.sleep(self._hello_retry_interval_seconds)

    async def _poll_status_loop(self) -> None:
        client = self._client
        server = self._server
        if client is None or server is None:
            return
        while server.session.is_connected and not self._stop_requested.is_set():
            try:
                response_object = await client.get_status(timeout_seconds=10.0)
                if isinstance(response_object, dict):
                    self._mark_ready()
                    self._emit_status(response_object)
                    status_object = response_object.get("status", {})
                    if isinstance(status_object, dict):
                        screen_name = safe_string(status_object.get("screen", "")).strip()
                        if screen_name:
                            server.session.last_screen = screen_name
                    self._emit_connection_state(True, self._ready_detected, server.session.last_screen)
            except Exception as exception:
                self._emit_log(f"Status poll error: {exception}")
            await asyncio.sleep(self._status_poll_interval_seconds)

    async def _songs_autoload_loop(self) -> None:
        start_time = time.time()
        server = self._server
        if server is None:
            return
        while server.session.is_connected and not self._stop_requested.is_set():
            if self._songs_loaded_once:
                return
            await self._refresh_songs_once(source_label="auto")
            if self._songs_loaded_once:
                return
            if time.time() - start_time > self._songs_missing_tip_after_seconds:
                screen_name = safe_string(server.session.last_screen or "")
                self._emit_log(f"No songs yet. Current screen={screen_name}. Go to ScreenSelectMusic and join P1.")
            await asyncio.sleep(self._songs_autoload_interval_seconds)

    async def _refresh_songs_once(self, source_label: str) -> None:
        client = self._client
        server = self._server
        if client is None or server is None or not server.session.is_connected or self._songs_refresh_lock is None:
            return
        async with self._songs_refresh_lock:
            self._emit_log(f"Refreshing songs ({source_label})")
            try:
                response_object = await client.get_songs(max_count=1000, group_filter="", timeout_seconds=60.0)
                self._mark_ready()
                songs_value = response_object.get("songs", [])
                if not isinstance(songs_value, list) or len(songs_value) == 0:
                    return
                ui_songs: List[UiSong] = []
                for song in songs_value:
                    if not isinstance(song, dict):
                        continue
                    difficulties_value = song.get("difficulties", [])
                    difficulties: List[str] = []
                    if isinstance(difficulties_value, list):
                        for item in difficulties_value:
                            difficulties.append(safe_string(item))
                    ui_songs.append(
                        UiSong(
                            song_dir=safe_string(song.get("song_dir", "")),
                            title=safe_string(song.get("title", "")),
                            artist=safe_string(song.get("artist", "")),
                            group=safe_string(song.get("group", "")),
                            difficulties=difficulties,
                        )
                    )
                if len(ui_songs) == 0:
                    return
                self._emit_songs(ui_songs)
                self._songs_loaded_once = True
                self._emit_log(f"Loaded {len(ui_songs)} songs")
            except Exception as exception:
                self._emit_log(f"Song refresh failed: {exception}")

    async def _start_song(self, song_dir: str, difficulty: str) -> None:
        client = self._client
        if client is None:
            return
        try:
            response_object = await client.start_song(song_dir=song_dir, difficulty=difficulty, timeout_seconds=15.0)
            self._mark_ready()
            self._emit_log(f"Start song response: {response_object}")
        except Exception as exception:
            self._emit_log(f"Start song failed: {exception}")

    async def _set_paused(self, paused: bool) -> None:
        client = self._client
        if client is None:
            return
        try:
            response_object = await client.set_paused(paused=paused, timeout_seconds=10.0)
            self._mark_ready()
            self._emit_log(f"Pause response: {response_object}")
        except Exception as exception:
            self._emit_log(f"Pause failed: {exception}")

    async def _stop_song(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            response_object = await client.stop(timeout_seconds=15.0)
            self._mark_ready()
            self._emit_log(f"Stop response: {response_object}")
        except Exception as exception:
            self._emit_log(f"Stop failed: {exception}")


class ItgmaniaPollingBridge:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        wire_debug: bool = WIRE_DEBUG_DEFAULT,
        logger: Optional[logging.Logger] = None,
        max_log_lines: int = 200,
    ) -> None:
        self._state_lock = threading.Lock()
        self._connection_state: Dict[str, Any] = {"connected": False, "ready": False, "screen": ""}
        self._last_status_response: Dict[str, Any] = {}
        self._songs: List[UiSong] = []
        self._songs_loaded_once = False
        self._last_log_line = ""
        self._log_lines: Deque[str] = deque(maxlen=max_log_lines)

        def on_connection_state(state: Dict[str, Any]) -> None:
            with self._state_lock:
                self._connection_state = dict(state)

        def on_status(status_response: Dict[str, Any]) -> None:
            with self._state_lock:
                self._last_status_response = dict(status_response)

        def on_songs(songs: List[UiSong]) -> None:
            with self._state_lock:
                self._songs = list(songs)
                self._songs_loaded_once = len(songs) > 0

        def on_log(message: str) -> None:
            with self._state_lock:
                self._last_log_line = message
                self._log_lines.append(message)

        def on_disconnect() -> None:
            with self._state_lock:
                self._last_status_response = {}
                self._songs = []
                self._songs_loaded_once = False

        self._runner = ItgmaniaHarnessRunner(
            host=host,
            port=port,
            logger=logger,
            wire_debug=wire_debug,
            on_connection_state=on_connection_state,
            on_status=on_status,
            on_songs=on_songs,
            on_log=on_log,
            on_disconnect=on_disconnect,
        )

    def stop(self) -> None:
        self._runner.stop()

    def request_refresh_songs(self) -> None:
        self._runner.request_refresh_songs()

    def request_start_song(self, song_dir: str, difficulty: str) -> None:
        self._runner.request_start_song(song_dir=song_dir, difficulty=difficulty)

    def request_set_paused(self, paused: bool) -> None:
        self._runner.request_set_paused(paused=paused)

    def request_stop_song(self) -> None:
        self._runner.request_stop_song()

    def get_state_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            return {"connection": dict(self._connection_state), "status_response": dict(self._last_status_response), "last_log_line": self._last_log_line, "logs": list(self._log_lines)}

    def get_songs_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            return {"songs": [song.to_dict() for song in self._songs], "songs_loaded_once": bool(self._songs_loaded_once)}


try:
    from PyQt6.QtCore import QObject, pyqtSignal
except Exception:  # pragma: no cover
    QObject = None  # type: ignore[assignment]
    pyqtSignal = None  # type: ignore[assignment]


if QObject is not None:

    class ItgmaniaQtEventBridge(QObject):  # type: ignore[misc]
        songs_updated = pyqtSignal(list)
        status_updated = pyqtSignal(dict)
        connection_updated = pyqtSignal(dict)
        log_message = pyqtSignal(str)

        def __init__(
            self,
            host: str = "127.0.0.1",
            port: int = 8765,
            *,
            wire_debug: bool = WIRE_DEBUG_DEFAULT,
            logger: Optional[logging.Logger] = None,
        ) -> None:
            super().__init__()

            def on_connection_state(state: Dict[str, Any]) -> None:
                self.connection_updated.emit(state)

            def on_status(status_response: Dict[str, Any]) -> None:
                self.status_updated.emit(status_response)

            def on_songs(songs: List[UiSong]) -> None:
                self.songs_updated.emit(songs)

            def on_log(message: str) -> None:
                self.log_message.emit(message)

            self._runner = ItgmaniaHarnessRunner(
                host=host,
                port=port,
                logger=logger,
                wire_debug=wire_debug,
                on_connection_state=on_connection_state,
                on_status=on_status,
                on_songs=on_songs,
                on_log=on_log,
            )

        def stop(self) -> None:
            self._runner.stop()

        def request_refresh_songs(self) -> None:
            self._runner.request_refresh_songs()

        def request_start_song(self, song_dir: str, difficulty: str) -> None:
            self._runner.request_start_song(song_dir=song_dir, difficulty=difficulty)

        def request_set_paused(self, paused: bool) -> None:
            self._runner.request_set_paused(paused=paused)

        def request_stop_song(self) -> None:
            self._runner.request_stop_song()

else:

    class ItgmaniaQtEventBridge:  # pragma: no cover
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError("PyQt6 is required to use ItgmaniaQtEventBridge")
