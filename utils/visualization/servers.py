"""CORS file servers for recorded trajectories and dashboard assets.

These servers keep the process working directory unchanged, register their
lifecycle for cleanup, and disable manifest caching while a run is recording.
"""

import os
import socket
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from threading import Event
from types import SimpleNamespace

from .registry import _register_visualization_server, _unregister_visualization_server
from .trajectories import latest_trajectory_output_dir_from_config, _trajectory_format_name


class CORSRequestHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def __init__(self, *args, directory=None, **kwargs):
            self.custom_directory = directory
            super().__init__(*args, directory=directory, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        if self.path.rstrip('/').endswith('manifest.json'):
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200, "ok")
        self.end_headers()

    def log_message(self, format, *args):
        """Suppress noisy dashboard polling access logs."""
        return


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64


def start_cors_http_server(directory, stop_event, port=8000):
    """Start a CORS-enabled HTTP server for the specified directory."""
    handler_class = lambda *args, **kwargs: CORSRequestHandler(*args, directory=directory, **kwargs)
    server_address = ('', port)
    httpd = ReusableThreadingHTTPServer(server_address, handler_class)
    httpd.timeout = 0.5

    def run_server():
        try:
            httpd.serve_forever(poll_interval=0.2)
        finally:
            httpd.server_close()
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.httpd = httpd
    server_thread.start()
    return server_thread


def run_visualization_server(data_folder, server_port = 8000):
    # store the current work directory
    # workdir = os.getcwd()
    # Ensure the data folder exists
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Created data folder: {data_folder}")
    
    # Start the HTTP server in a separate thread
    # os.chdir(data_folder)  # Change to the specified directory
    stop_event = Event() 
    server_thread = start_cors_http_server(data_folder, stop_event, server_port)
    _register_visualization_server(stop_event, server_thread, port=server_port, directory=data_folder)
    print(f"Serving {data_folder} with CORS enabled on port {server_port}...")

    # recovery the work directory
    # os.chdir(workdir)

    return stop_event, server_thread


def stop_visualization_server(stop_event, server_thread, port=8000, join_timeout=2.0, verbose=True):
    if stop_event is not None:
        stop_event.set()
    httpd = getattr(server_thread, "httpd", None)
    if httpd is not None:
        httpd.shutdown()

    # Send dummy request to unblock handle_request()
    if httpd is None:
        try:
            with socket.create_connection(("localhost", port), timeout=1) as sock:
                sock.sendall(b"HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        except Exception:
            pass

    if server_thread is None:
        _unregister_visualization_server(stop_event=stop_event)
        return
    server_thread.join(timeout=join_timeout)
    _unregister_visualization_server(server_thread=server_thread, stop_event=stop_event)
    if server_thread.is_alive():
        if verbose:
            print(f"Visualization server thread did not stop within {join_timeout:.1f} seconds.")
    else:
        if verbose:
            print("Visualization server stopped.")


def start_visualization_server_from_config(
        config,
        sim_index=0,
        trajectory_output_dir=None,
        server_port=8000,
        prefer_binary=True,
        wait_seconds=0,
        output_root="output"):
    latest_directory = latest_trajectory_output_dir_from_config(
        config,
        sim_index=sim_index,
        trajectory_output_dir=trajectory_output_dir,
        prefer_binary=prefer_binary,
        wait_seconds=wait_seconds,
        output_root=output_root,
    )
    print(
        f"Starting visualization server for {_trajectory_format_name(latest_directory)} "
        f"trajectory output: {latest_directory}"
    )
    stop_event, server_thread = run_visualization_server(latest_directory, server_port)

    def stop():
        stop_visualization_server(stop_event, server_thread, server_port)

    return SimpleNamespace(
        directory=latest_directory,
        port=server_port,
        stop_event=stop_event,
        server_thread=server_thread,
        stop=stop,
    )
