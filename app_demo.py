import argparse
import asyncio
import logging
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from PyQt6.QtCore import QObject, Qt, QTimer, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QPushButton,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from itgmania_comms import ItgmaniClient, ItgmaniHarnessServer, ItgmaniTextEvent


WIRE_DEBUG = False


logging.basicConfig(level=logging.INFO)


def _safe_string(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return ""



def _parse_kv_fields(text: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for part in text.split("|"):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        fields[key.strip()] = value.strip()
    return fields


def _screen_from_heartbeat_value(value: str) -> str:
    fields = _parse_kv_fields(value)
    return fields.get("screen", "").strip()


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


class ItgmaniQtBridge(QObject):
    songs_updated = pyqtSignal(list)
    status_updated = pyqtSignal(dict)
    connection_updated = pyqtSignal(dict)
    log_message = pyqtSignal(str)

    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        super().__init__()
        self._host = host
        self._port = port

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
        self._runner.submit(self._start_song(song_dir, difficulty))

    def request_set_paused(self, paused: bool) -> None:
        self._runner.submit(self._set_paused(paused))

    def request_stop_song(self) -> None:
        self._runner.submit(self._stop_song())

    def _emit_log(self, message: str) -> None:
        logging.info(message)
        self.log_message.emit(message)

    def _mark_ready(self) -> None:
        if not self._ready_detected:
            self._ready_detected = True

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
            if self._server is None:
                return

            if text_event.kind == "heartbeat":
                self._mark_ready()
                screen_name = _screen_from_heartbeat_value(_safe_string(text_event.value))
                if screen_name:
                    self._server.session.last_screen = screen_name
                self.connection_updated.emit(
                    {
                        "connected": True,
                        "ready": self._ready_detected,
                        "screen": self._server.session.last_screen,
                    }
                )

            if text_event.kind == "screen":
                screen_name = _screen_from_screen_value(_safe_string(text_event.value))
                if screen_name:
                    self._server.session.last_screen = screen_name
                self.connection_updated.emit(
                    {
                        "connected": True,
                        "ready": self._ready_detected,
                        "screen": self._server.session.last_screen,
                    }
                )

        async def on_disconnect() -> None:
            self._ready_detected = False
            self._songs_loaded_once = False
            self.songs_updated.emit([])
            self.status_updated.emit({})
            self.connection_updated.emit({"connected": False, "ready": False, "screen": ""})

        self._server.session.set_text_event_handler(on_text_event)
        self._server.session.set_disconnect_handler(on_disconnect)

        while not self._stop_requested.is_set():
            self._ready_detected = False
            self._songs_loaded_once = False

            self.connection_updated.emit({"connected": False, "ready": False, "screen": ""})
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
            self.connection_updated.emit({"connected": True, "ready": False, "screen": self._server.session.last_screen})
            self._emit_log("ITGmania connected")

            poll_task = asyncio.create_task(self._poll_status_loop())
            hello_task = asyncio.create_task(self._hello_loop())
            songs_task = asyncio.create_task(self._songs_autoload_loop())

            try:
                while self._server.session.is_connected and not self._stop_requested.is_set():
                    self.connection_updated.emit({"connected": True, "ready": self._ready_detected, "screen": self._server.session.last_screen})
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

        while self._server is not None and self._server.session.is_connected and not self._stop_requested.is_set():
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

        while self._server is not None and self._server.session.is_connected and not self._stop_requested.is_set():
            try:
                response_object = await client.get_status(timeout_seconds=10.0)
                if isinstance(response_object, dict):
                    self._mark_ready()
                    self.status_updated.emit(response_object)

                    status_object = response_object.get("status", {})
                    if isinstance(status_object, dict):
                        screen_name = _safe_string(status_object.get("screen", "")).strip()
                        if screen_name:
                            self._server.session.last_screen = screen_name

                    self.connection_updated.emit({"connected": True, "ready": self._ready_detected, "screen": self._server.session.last_screen})
            except Exception as exception:
                self._emit_log(f"Status poll error: {exception}")

            await asyncio.sleep(0.25)

    async def _songs_autoload_loop(self) -> None:
        start_time = time.time()
        while self._server is not None and self._server.session.is_connected and not self._stop_requested.is_set():
            if self._songs_loaded_once:
                return

            await self._refresh_songs_once(source_label="auto")

            if self._songs_loaded_once:
                return

            elapsed = time.time() - start_time
            if elapsed > 2.0:
                screen_name = self._server.session.last_screen or ""
                self._emit_log(f"No songs yet. Current screen={screen_name}. Go to ScreenSelectMusic and join P1.")

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
                response_object = await client.get_songs(max_count=1000, group_filter="", timeout_seconds=60.0)
                self._mark_ready()

                songs = response_object.get("songs", [])
                if not isinstance(songs, list) or len(songs) == 0:
                    return

                ui_songs: List[UiSong] = []
                for song in songs:
                    if not isinstance(song, dict):
                        continue

                    song_dir = _safe_string(song.get("song_dir", ""))
                    title = _safe_string(song.get("title", ""))
                    artist = _safe_string(song.get("artist", ""))
                    group = _safe_string(song.get("group", ""))

                    difficulties_value = song.get("difficulties", [])
                    difficulties: List[str] = []
                    if isinstance(difficulties_value, list):
                        for item in difficulties_value:
                            difficulties.append(_safe_string(item))

                    ui_songs.append(
                        UiSong(
                            song_dir=song_dir,
                            title=title,
                            artist=artist,
                            group=group,
                            difficulties=difficulties,
                        )
                    )

                self.songs_updated.emit(ui_songs)
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


class MainWindow(QMainWindow):
    def __init__(self, host: str, port: int) -> None:
        super().__init__()
        self.setWindowTitle("ITGmania Harness Demo")

        self._bridge = ItgmaniQtBridge(host=host, port=port)
        self._bridge.songs_updated.connect(self._on_songs_updated)
        self._bridge.status_updated.connect(self._on_status_updated)
        self._bridge.connection_updated.connect(self._on_connection_updated)
        self._bridge.log_message.connect(self._on_log_message)

        self._songs_list = QListWidget()
        self._songs_list.currentItemChanged.connect(self._on_song_selection_changed)

        self._difficulty_combo = QComboBox()
        self._difficulty_combo.setEnabled(False)

        self._start_button = QPushButton("Start")
        self._pause_button = QPushButton("Pause")
        self._stop_button = QPushButton("Stop")
        self._refresh_button = QPushButton("Refresh songs")

        self._start_button.clicked.connect(self._start_clicked)
        self._pause_button.clicked.connect(self._pause_clicked)
        self._stop_button.clicked.connect(self._stop_clicked)
        self._refresh_button.clicked.connect(self._refresh_clicked)

        self._start_button.setEnabled(False)
        self._pause_button.setEnabled(False)
        self._stop_button.setEnabled(False)

        root_widget = QWidget()
        root_layout = QVBoxLayout(root_widget)

        root_layout.addWidget(QLabel("Song list"))
        root_layout.addWidget(self._songs_list)

        difficulty_layout = QHBoxLayout()
        difficulty_layout.addWidget(QLabel("Difficulty"))
        difficulty_layout.addWidget(self._difficulty_combo, 1)
        root_layout.addLayout(difficulty_layout)

        controls_layout = QHBoxLayout()
        controls_layout.addWidget(self._start_button)
        controls_layout.addWidget(self._pause_button)
        controls_layout.addWidget(self._stop_button)
        controls_layout.addStretch(1)
        controls_layout.addWidget(self._refresh_button)
        root_layout.addLayout(controls_layout)

        self.setCentralWidget(root_widget)

        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)

        self._connection_state: Dict[str, Any] = {"connected": False, "ready": False, "screen": ""}
        self._last_status_object: Dict[str, Any] = {}
        self._last_log_line: str = ""

        self._ui_timer = QTimer(self)
        self._ui_timer.setInterval(250)
        self._ui_timer.timeout.connect(self._refresh_status_bar)
        self._ui_timer.start()

        self._refresh_status_bar()

    def closeEvent(self, event) -> None:
        try:
            self._bridge.stop()
        except Exception:
            pass
        super().closeEvent(event)

    def _on_log_message(self, message: str) -> None:
        self._last_log_line = message
        self._refresh_status_bar()

    def _on_connection_updated(self, state: Dict[str, Any]) -> None:
        self._connection_state = state
        self._update_control_states()

    def _on_status_updated(self, response_object: Dict[str, Any]) -> None:
        self._last_status_object = response_object
        self._update_control_states()

    def _update_control_states(self) -> None:
        connected = bool(self._connection_state.get("connected", False))
        ready = bool(self._connection_state.get("ready", False))
        has_selection = self._songs_list.currentItem() is not None

        self._start_button.setEnabled(connected and ready and has_selection)
        self._difficulty_combo.setEnabled(connected and ready and has_selection)

        status_object = self._last_status_object.get("status", {})
        paused = False
        if isinstance(status_object, dict):
            paused = bool(status_object.get("paused", False))

        self._pause_button.setEnabled(connected and ready)
        self._stop_button.setEnabled(connected and ready)
        self._pause_button.setText("Resume" if paused else "Pause")

    def _on_songs_updated(self, ui_songs: List[UiSong]) -> None:
        self._songs_list.clear()
        for ui_song in ui_songs:
            display_parts: List[str] = []
            if ui_song.title:
                display_parts.append(ui_song.title)
            if ui_song.artist:
                display_parts.append(f"by {ui_song.artist}")
            if ui_song.group:
                display_parts.append(f"[{ui_song.group}]")

            display_text = " ".join(display_parts).strip() or ui_song.song_dir
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, ui_song)
            self._songs_list.addItem(item)

        if self._songs_list.count() > 0:
            self._songs_list.setCurrentRow(0)

        self._update_control_states()

    def _on_song_selection_changed(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:
        self._difficulty_combo.clear()

        ui_song = None
        if current is not None:
            ui_song = current.data(Qt.ItemDataRole.UserRole)

        if not isinstance(ui_song, UiSong):
            self._difficulty_combo.setEnabled(False)
            self._update_control_states()
            return

        for difficulty_name in ui_song.difficulties:
            self._difficulty_combo.addItem(difficulty_name)

        preferred = "Difficulty_Easy"
        preferred_index = self._difficulty_combo.findText(preferred)
        if preferred_index >= 0:
            self._difficulty_combo.setCurrentIndex(preferred_index)
        elif self._difficulty_combo.count() > 0:
            self._difficulty_combo.setCurrentIndex(0)

        self._update_control_states()

    def _selected_song(self) -> Optional[UiSong]:
        current_item = self._songs_list.currentItem()
        if current_item is None:
            return None
        value = current_item.data(Qt.ItemDataRole.UserRole)
        if isinstance(value, UiSong):
            return value
        return None

    def _start_clicked(self) -> None:
        ui_song = self._selected_song()
        if ui_song is None:
            return
        difficulty = self._difficulty_combo.currentText().strip() or "Difficulty_Easy"
        self._bridge.request_start_song(song_dir=ui_song.song_dir, difficulty=difficulty)

    def _pause_clicked(self) -> None:
        status_object = self._last_status_object.get("status", {})
        paused = False
        if isinstance(status_object, dict):
            paused = bool(status_object.get("paused", False))
        self._bridge.request_set_paused(not paused)

    def _stop_clicked(self) -> None:
        self._bridge.request_stop_song()

    def _refresh_clicked(self) -> None:
        self._bridge.request_refresh_songs()

    def _refresh_status_bar(self) -> None:
        connected = bool(self._connection_state.get("connected", False))
        ready = bool(self._connection_state.get("ready", False))
        screen_name = _safe_string(self._connection_state.get("screen", "")).strip()

        status_object = self._last_status_object.get("status", {})
        playing = False
        paused = False
        current_title = ""
        if isinstance(status_object, dict):
            playing = bool(status_object.get("is_playing", False))
            paused = bool(status_object.get("paused", False))
            current_title = _safe_string(status_object.get("current_title", "")).strip()
            if not screen_name:
                screen_name = _safe_string(status_object.get("screen", "")).strip()

        parts: List[str] = [
            f"ITGmania {'connected' if connected else 'disconnected'}",
            f"{'ready' if ready else 'not-ready'}",
        ]
        if screen_name:
            parts.append(f"screen={screen_name}")
        parts.append(f"playing={1 if playing else 0}")
        parts.append(f"paused={1 if paused else 0}")
        if current_title:
            parts.append(f"title={current_title}")
        if self._last_log_line:
            parts.append(self._last_log_line)

        self._status_bar.showMessage(" | ".join(parts))


def main() -> int:
    parser = argparse.ArgumentParser(description="ITGmania Harness Demo (PyQt)")
    parser.add_argument("--host", default="127.0.0.1", help="Harness websocket bind host")
    parser.add_argument("--port", type=int, default=8765, help="Harness websocket bind port")
    arguments, qt_args = parser.parse_known_args(sys.argv[1:])

    app = QApplication([sys.argv[0]] + qt_args)
    window = MainWindow(host=arguments.host, port=arguments.port)
    window.resize(900, 600)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
