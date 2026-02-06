from __future__ import annotations

import argparse
import logging
from typing import Any

from flask import Flask, jsonify, render_template, request

from itgmania_comm_wrapper import ItgmaniaPollingBridge, safe_string


logging.basicConfig(level=logging.INFO)


def create_app(bridge: ItgmaniaPollingBridge) -> Flask:
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
        bridge.request_set_paused(paused=bool(payload.get("paused", False)))
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

    bridge = ItgmaniaPollingBridge(host=arguments.harness_host, port=arguments.harness_port)
    app = create_app(bridge)

    try:
        # Disable the reloader so the harness thread does not start twice.
        app.run(host=arguments.web_host, port=arguments.web_port, debug=False, threaded=True, use_reloader=False)
    finally:
        bridge.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
