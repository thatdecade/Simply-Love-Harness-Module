import argparse
import logging
import sys
from typing import Any, Dict, List, Optional

from PyQt6.QtCore import Qt, QTimer
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

from itgmania_comm_wrapper import ItgmaniaQtEventBridge as ItgmaniQtBridge, UiSong, safe_string as _safe_string


logging.basicConfig(level=logging.INFO)

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
