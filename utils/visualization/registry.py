"""Track process-local visualization servers and their owning clients.

Weak client references do not extend client lifetimes. Registrations are guarded
by one lock; shutdown operates on snapshots so client callbacks may unregister.
"""

import os
import threading
import weakref


_SERVER_REGISTRY_LOCK = threading.RLock()


_VISUALIZATION_SERVER_REGISTRY = []


_METSR_CLIENT_REGISTRY = weakref.WeakSet()


def register_metsr_client(client):
    """Track a METSRClient so process-local helper servers can be stopped."""
    with _SERVER_REGISTRY_LOCK:
        _METSR_CLIENT_REGISTRY.add(client)
    return client


def unregister_metsr_client(client):
    with _SERVER_REGISTRY_LOCK:
        try:
            _METSR_CLIENT_REGISTRY.discard(client)
        except TypeError:
            pass


def _register_visualization_server(stop_event, server_thread, port=None, directory=None):
    entry = {
        "stop_event": stop_event,
        "server_thread": server_thread,
        "port": int(port) if port is not None else None,
        "directory": os.path.abspath(directory) if directory else None,
    }
    with _SERVER_REGISTRY_LOCK:
        _VISUALIZATION_SERVER_REGISTRY.append(entry)
    return entry


def _unregister_visualization_server(server_thread=None, stop_event=None):
    with _SERVER_REGISTRY_LOCK:
        _VISUALIZATION_SERVER_REGISTRY[:] = [
            entry for entry in _VISUALIZATION_SERVER_REGISTRY
            if not (
                (server_thread is not None and entry.get("server_thread") is server_thread)
                or (stop_event is not None and entry.get("stop_event") is stop_event)
            )
        ]


def stop_all_visualization_servers(verbose=True, join_timeout=2.0):
    """Stop all file/CORS visualization servers started in this Python process."""
    # Imported at shutdown time to keep registry and server modules acyclic.
    from .servers import stop_visualization_server

    with _SERVER_REGISTRY_LOCK:
        entries = list(_VISUALIZATION_SERVER_REGISTRY)
    stopped = []
    for entry in entries:
        record = {
            "port": entry.get("port"),
            "directory": entry.get("directory"),
            "returncode": 0,
            "stderr": "",
        }
        try:
            stop_visualization_server(
                entry.get("stop_event"),
                entry.get("server_thread"),
                port=entry.get("port") or 8000,
                join_timeout=join_timeout,
                verbose=verbose,
            )
        except Exception as exc:
            record["returncode"] = 1
            record["stderr"] = str(exc).splitlines()[0]
            if verbose:
                print(f"Failed to stop visualization server on port {record['port']}: {record['stderr']}")
        stopped.append(record)
    if verbose and not stopped:
        print("No process-local visualization file servers found.")
    return stopped


def stop_all_metsr_client_servers(verbose=True):
    """Stop live stream and file servers owned by registered METSRClient objects."""
    with _SERVER_REGISTRY_LOCK:
        clients = list(_METSR_CLIENT_REGISTRY)
    stopped = []
    for client in clients:
        record = {
            "client": repr(client),
            "had_stream_server": getattr(client, "viz_stream_server", None) is not None,
            "had_file_server": getattr(client, "viz_server", None) is not None,
            "returncode": 0,
            "stderr": "",
        }
        if not record["had_stream_server"] and not record["had_file_server"]:
            continue
        try:
            stop_viz = getattr(client, "stop_viz", None)
            if callable(stop_viz):
                try:
                    stop_viz(verbose=verbose)
                except TypeError:
                    stop_viz()
            else:
                if record["had_stream_server"] and hasattr(client, "stop_viz_stream"):
                    client.stop_viz_stream()
                if record["had_file_server"] and hasattr(client, "stop_offline_viz"):
                    client.stop_offline_viz()
            if verbose:
                print(
                    "Stopped METS-R client helper servers "
                    f"stream={record['had_stream_server']} file={record['had_file_server']}"
                )
        except Exception as exc:
            record["returncode"] = 1
            record["stderr"] = str(exc).splitlines()[0]
            if verbose:
                print(f"Failed to stop METS-R client helper servers: {record['stderr']}")
        stopped.append(record)
    if verbose and not stopped:
        print("No registered METS-R client helper servers found.")
    return stopped
