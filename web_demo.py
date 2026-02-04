from __future__ import annotations

import argparse
import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional

from flask import Flask, jsonify, render_template, request

from itgmania_comms import ItgmaniClient, ItgmaniHarnessServer, ItgmaniTextEvent


WIRE_DEBUG = False

logging.basicConfig(level=logging.INFO)


def safe_string(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return ""


@dataclass(frozen=True)
class UiSong:
    song_dir: str
    title: str
    artist: str
    group: str
    difficulties: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "song_dir": self.song_dir,
            "title": self.title,
            "artist": self.artist,
            "group": self.group,
            "difficulties": list(self.difficulties),
        }


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

        self._thread = threading.Thread(
            target=thread_target,
            name="asyncio-runner",
            daemon=True,
        )
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


class ItgmaniWebBridge:
    """
    Web-friendly bridge that preserves the same harness behavior as the PyQt demo:
    - Starts an ItgmaniHarnessServer and waits for ITGmania to connect
    - Runs HELLO retries until ready
    - Polls status every 250 ms
    - Auto-loads songs once per connection, with manual refresh support
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        self._host = host
        self._port = port

        self._runner = AsyncioThreadRunner()
        self._runner.start()

        self._server: Optional[ItgmaniHarnessServer] = None
        self._client: Optional[ItgmaniClient] = None

        self._stop_requested = threading.Event()
        self._ready_detected = False
        self._songs_loaded_once = False

        self._state_lock = threading.Lock()
        self._connection_state: Dict[str, Any] = {"connected": False, "ready": False, "screen": ""}
        self._last_status_response: Dict[str, Any] = {}
        self._songs: List[UiSong] = []
        self._last_log_line = ""
        self._log_lines: Deque[str] = deque(maxlen=200)

        # Created inside the asyncio loop after startup.
        self._songs_refresh_lock: Optional[asyncio.Lock] = None

        self._runner.submit(self._run())

    def stop(self) -> None:
        self._stop_requested.set()
        self._runner.submit(self._shutdown())

    def request_refresh_songs(self) -> None:
        self._runner.submit(self._refresh_songs_once(source_label="manual"))

    def request_start_song(self, song_dir: str, difficulty: str) -> None:
        self._runner.submit(self._start_song(song_dir, difficulty))

    def request_set_paused(self, paused: bool) -> None:
        self._runner.submit(self._set_paused(paused))

    def request_stop_song(self) -> None:
        self._runner.submit(self._stop_song())

    def get_state_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            return {
                "connection": dict(self._connection_state),
                "status_response": dict(self._last_status_response),
                "last_log_line": self._last_log_line,
                "logs": list(self._log_lines),
            }

    def get_songs_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            return {
                "songs": [song.to_dict() for song in self._songs],
                "songs_loaded_once": self._songs_loaded_once,
            }

    def _emit_log(self, message: str) -> None:
        logging.info(message)
        with self._state_lock:
            self._last_log_line = message
            self._log_lines.append(message)

    def _mark_ready(self) -> None:
        if not self._ready_detected:
            self._ready_detected = True

    def _set_connection_state(self, connected: bool, ready: bool, screen: str) -> None:
        with self._state_lock:
            self._connection_state = {"connected": connected, "ready": ready, "screen": screen}

    async def _run(self) -> None:
        self._songs_refresh_lock = asyncio.Lock()

        self._server = ItgmaniHarnessServer(
            host=self._host,
            port=self._port,
            logger=logging.getLogger("itgmani"),
            wire_debug=WIRE_DEBUG,
        )
        await self._server.start()
        self._client = ItgmaniClient(self._server.session)

        async def on_text_event(text_event: ItgmaniTextEvent) -> None:
            if text_event.kind == "heartbeat":
                self._mark_ready()
            if text_event.kind == "screen":
                self._set_connection_state(
                    connected=True,
                    ready=self._ready_detected,
                    screen=safe_string(text_event.value),
                )

        async def on_disconnect() -> None:
            self._ready_detected = False
            self._songs_loaded_once = False
            with self._state_lock:
                self._last_status_response = {}
                self._songs = []
            self._set_connection_state(connected=False, ready=False, screen="")

        self._server.session.set_text_event_handler(on_text_event)
        self._server.session.set_disconnect_handler(on_disconnect)

        while not self._stop_requested.is_set():
            self._ready_detected = False
            self._songs_loaded_once = False
            with self._state_lock:
                self._last_status_response = {}
                self._songs = []

            self._set_connection_state(connected=False, ready=False, screen="")
            self._emit_log(f"Listening on ws://{self._host}:{self._port} and waiting for ITGmania to connect")

            await self._server.session.connected_event.wait()
            self._set_connection_state(
                connected=True,
                ready=False,
                screen=safe_string(self._server.session.last_screen),
            )
            self._emit_log("ITGmania connected")

            poll_task = asyncio.create_task(self._poll_status_loop())
            hello_task = asyncio.create_task(self._hello_loop())
            songs_task = asyncio.create_task(self._songs_autoload_loop())

            try:
                while self._server.session.is_connected and not self._stop_requested.is_set():
                    self._set_connection_state(
                        connected=True,
                        ready=self._ready_detected,
                        screen=safe_string(self._server.session.last_screen),
                    )
                    await asyncio.sleep(0.25)
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
        if self._server is not None:
            try:
                await self._server.stop()
            except Exception:
                pass

    async def _hello_loop(self) -> None:
        client = self._client
        if client is None:
            return

        while (
            self._server is not None
            and self._server.session.is_connected
            and not self._stop_requested.is_set()
        ):
            if self._ready_detected:
                return
            try:
                response_object = await client.hello(max_attempts=1)
                self._mark_ready()
                self._emit_log(f"HELLO ok: {response_object}")
                return
            except Exception as exception:
                self._emit_log(f"HELLO retry: {exception}")
            await asyncio.sleep(1.0)

    async def _poll_status_loop(self) -> None:
        client = self._client
        if client is None:
            return

        while (
            self._server is not None
            and self._server.session.is_connected
            and not self._stop_requested.is_set()
        ):
            try:
                response_object = await client.get_status(timeout_seconds=10.0)
                if isinstance(response_object, dict):
                    self._mark_ready()
                    with self._state_lock:
                        self._last_status_response = response_object

                    status_object = response_object.get("status", {})
                    if isinstance(status_object, dict):
                        screen_name = safe_string(status_object.get("screen", "")).strip()
                        if screen_name:
                            self._server.session.last_screen = screen_name

                    self._set_connection_state(
                        connected=True,
                        ready=self._ready_detected,
                        screen=safe_string(self._server.session.last_screen),
                    )
            except Exception as exception:
                self._emit_log(f"Status poll error: {exception}")

            await asyncio.sleep(0.25)

    async def _songs_autoload_loop(self) -> None:
        start_time = time.time()
        while (
            self._server is not None
            and self._server.session.is_connected
            and not self._stop_requested.is_set()
        ):
            if self._songs_loaded_once:
                return

            await self._refresh_songs_once(source_label="auto")

            if self._songs_loaded_once:
                return

            elapsed = time.time() - start_time
            if elapsed > 2.0:
                screen_name = safe_string(self._server.session.last_screen or "")
                self._emit_log(
                    f"No songs yet. Current screen={screen_name}. Go to ScreenSelectMusic and join P1."
                )

            await asyncio.sleep(1.0)

    async def _refresh_songs_once(self, source_label: str) -> None:
        client = self._client
        if client is None:
            return
        if self._server is None or not self._server.session.is_connected:
            return

        if self._songs_refresh_lock is None:
            return

        async with self._songs_refresh_lock:
            self._emit_log(f"Refreshing songs ({source_label})")

            try:
                response_object = await client.get_songs(
                    max_count=1000,
                    group_filter="",
                    timeout_seconds=60.0,
                )
                self._mark_ready()

                songs_value = response_object.get("songs", [])
                if not isinstance(songs_value, list) or len(songs_value) == 0:
                    return

                ui_songs: List[UiSong] = []
                for song in songs_value:
                    if not isinstance(song, dict):
                        continue

                    song_dir = safe_string(song.get("song_dir", ""))
                    title = safe_string(song.get("title", ""))
                    artist = safe_string(song.get("artist", ""))
                    group = safe_string(song.get("group", ""))

                    difficulties_value = song.get("difficulties", [])
                    difficulties: List[str] = []
                    if isinstance(difficulties_value, list):
                        for item in difficulties_value:
                            difficulties.append(safe_string(item))

                    ui_songs.append(
                        UiSong(
                            song_dir=song_dir,
                            title=title,
                            artist=artist,
                            group=group,
                            difficulties=difficulties,
                        )
                    )

                with self._state_lock:
                    self._songs = ui_songs
                    self._songs_loaded_once = True

                self._emit_log(f"Loaded {len(ui_songs)} songs")

            except Exception as exception:
                self._emit_log(f"Song refresh failed: {exception}")

    async def _start_song(self, song_dir: str, difficulty: str) -> None:
        client = self._client
        if client is None:
            return
        try:
            response_object = await client.start_song(
                song_dir=song_dir,
                difficulty=difficulty,
                timeout_seconds=15.0,
            )
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


def create_app(bridge: ItgmaniWebBridge) -> Flask:
    app = Flask(__name__, template_folder="templates")

    @app.get("/")
    def index() -> Any:
        return render_template("index.html")

    @app.get("/api/state")
    def api_state() -> Any:
        return jsonify(bridge.get_state_snapshot())

    @app.get("/api/songs")
    def api_songs() -> Any:
        return jsonify(bridge.get_songs_snapshot())

    @app.post("/api/refresh_songs")
    def api_refresh_songs() -> Any:
        bridge.request_refresh_songs()
        return jsonify({"ok": True})

    @app.post("/api/start_song")
    def api_start_song() -> Any:
        payload = request.get_json(silent=True) or {}
        song_dir = safe_string(payload.get("song_dir", "")).strip()
        difficulty = safe_string(payload.get("difficulty", "")).strip() or "Difficulty_Easy"
        if not song_dir:
            return jsonify({"ok": False, "error": "song_dir is required"}), 400
        bridge.request_start_song(song_dir=song_dir, difficulty=difficulty)
        return jsonify({"ok": True})

    @app.post("/api/set_paused")
    def api_set_paused() -> Any:
        payload = request.get_json(silent=True) or {}
        paused_value = payload.get("paused", False)
        paused = bool(paused_value)
        bridge.request_set_paused(paused=paused)
        return jsonify({"ok": True})

    @app.post("/api/stop_song")
    def api_stop_song() -> Any:
        bridge.request_stop_song()
        return jsonify({"ok": True})

    return app


def main() -> int:
    parser = argparse.ArgumentParser(description="ITGmania Harness Web Demo (Flask)")
    parser.add_argument("--harness-host", default="127.0.0.1", help="Harness websocket bind host")
    parser.add_argument("--harness-port", type=int, default=8765, help="Harness websocket bind port")
    parser.add_argument("--web-host", default="127.0.0.1", help="Flask bind host")
    parser.add_argument("--web-port", type=int, default=5000, help="Flask bind port")
    arguments = parser.parse_args()

    bridge = ItgmaniWebBridge(host=arguments.harness_host, port=arguments.harness_port)
    app = create_app(bridge)

    try:
        # Disable the reloader so the harness thread does not start twice.
        app.run(host=arguments.web_host, port=arguments.web_port, debug=False, threaded=True, use_reloader=False)
    finally:
        bridge.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
