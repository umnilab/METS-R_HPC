"""Control APIs for simulation."""

import json
import math
import time
import uuid

from utils.primitives import _as_list


class ControlSimulationMixin:
    """Control APIs for simulation."""

    def tick(
            self,
            step_num = 1,
            wait_forever = False,
            retry_interval = None,
            max_wait_seconds = None,
            poll_timeout = 5,
            max_stalled_seconds = None):
        """Advance the simulator and wait until the requested tick is reached.

        ``wait_forever`` keeps tolerating slow steps, but it should not hide a
        dead server or a permanently stalled tick. Progress is therefore based
        on the server tick actually increasing, not just on receiving a reply.
        Once the tick advances beyond the tick attached to a STEP request, that
        request is considered active and is never resent.
        """
        assert self.current_tick is not None, "self.current_tick is None. Maybe there is another METS-R SIM instance unclosed."

        step_num = int(step_num)
        if step_num < 1:
            raise ValueError("step_num must be a positive integer")

        poll_timeout = min(max(0.1, float(poll_timeout)), max(0.1, float(self.timeout)))

        with self.lock:
            start_tick = int(self.current_tick)
            target_tick = start_tick + step_num
            overall_start = time.time()
            last_progress_time = overall_start
            last_send_time = overall_start
            step_request_tick = None

            if retry_interval is None:
                retry_interval = min(float(self.timeout), 30.0)

            if not wait_forever and max_wait_seconds is None:
                max_wait_seconds = self.timeout

            if wait_forever and max_stalled_seconds is None:
                max_stalled_seconds = max(60.0, min(float(self.timeout), 300.0))

            def send_step_request():
                nonlocal last_send_time, step_request_tick
                remaining_steps = target_tick - int(self.current_tick)
                if remaining_steps <= 0:
                    return
                msg = {"messageType": "step", "tick": int(self.current_tick), "tickCount": remaining_steps}
                self.send_msg(msg)
                step_request_tick = int(self.current_tick)
                last_send_time = time.time()

            def step_request_is_active():
                return (
                    step_request_tick is not None
                    and int(self.current_tick) > step_request_tick
                )

            server_tick_started = time.perf_counter()
            send_step_request()

            while True:
                if int(self.current_tick) >= target_tick:
                    break

                if max_wait_seconds is not None and time.time() - overall_start > max_wait_seconds:
                    raise TimeoutError(
                        f"Timed out waiting for METS-R SIM to reach tick {target_tick}; "
                        f"last received tick was {self.current_tick}"
                    )

                # Always use a bounded receive here so wait_forever=True can still retry
                # the STEP request instead of blocking forever on a missed heartbeat.
                tick_before_receive = int(self.current_tick)
                res = self.receive_msg(
                    ignore_heartbeats=False,
                    waiting_forever=False,
                    print_timeout=False,
                    timeout=poll_timeout,
                )
                now = time.time()
                if int(self.current_tick) > tick_before_receive:
                    last_progress_time = now

                if int(self.current_tick) >= target_tick:
                    break

                if (
                        max_stalled_seconds is not None
                        and now - last_progress_time > max_stalled_seconds):
                    raise TimeoutError(
                        f"METS-R SIM made no tick progress for "
                        f"{now - last_progress_time:.1f} seconds while waiting for "
                        f"tick {target_tick}; last server tick was {self.current_tick}. "
                        "The simulator or WebSocket server is likely stalled."
                    )

                if res is None:
                    tick_before_query = int(self.current_tick)
                    synced_tick = self._query_tick_locked()
                    now = time.time()
                    if synced_tick is not None and int(self.current_tick) > tick_before_query:
                        last_progress_time = now

                    if int(self.current_tick) >= target_tick:
                        break

                    # A successful tick query may overtake the heartbeat that
                    # timed out. Progress past the request tick proves that the
                    # existing STEP was accepted and is still running toward
                    # target_tick, so resending here would be a stale duplicate.
                    if step_request_is_active():
                        continue

                    if not wait_forever:
                        raise TimeoutError(
                            f"Timed out waiting for METS-R SIM to reach tick {target_tick}; "
                            f"last received tick was {self.current_tick}"
                        )

                    should_retry_step = now - last_send_time >= retry_interval
                    if should_retry_step:
                        if self.verbose:
                            print(
                                f"Still waiting for STEP tick {target_tick}; "
                                f"last received tick was {self.current_tick}. Retrying STEP request."
                            )
                        send_step_request()
                    continue

                if res["messageType"] == "tick":
                    continue

                if res["messageType"] != "step":
                    raise RuntimeError(f"Expected STEP while ticking, received {res['messageType']}")

                if res.get("status") == "error":
                    tick_before_query = int(self.current_tick)
                    synced_tick = self._query_tick_locked()
                    now = time.time()
                    if synced_tick is not None and int(self.current_tick) > tick_before_query:
                        last_progress_time = now
                    if int(self.current_tick) >= target_tick:
                        break
                    if (
                            res.get('errorCode') == 'TICK_MISMATCH'
                            and step_request_is_active()):
                        # A stale duplicate may already be queued from an older
                        # client retry. The original STEP is still active; wait
                        # for it instead of creating another duplicate.
                        continue
                    if not wait_forever:
                        raise RuntimeError(
                            f"METS-R SIM rejected STEP request for tick {target_tick}; "
                            f"server reported tick {self.current_tick}"
                        )
                    send_step_request()
                    continue

                step_tick = int(res["tick"])
                if step_tick < int(self.current_tick):
                    continue

                if step_tick > int(self.current_tick):
                    self.current_tick = step_tick
                    last_progress_time = now

                if step_tick >= target_tick:
                    break

        self._record_server_tick_time(server_tick_started, start_tick)

    def advance_seconds(self, seconds, step_size=None, **tick_kwargs):
        """Advance by at least ``seconds`` using the configured simulation step."""
        seconds = float(seconds)
        if seconds < 0.0:
            raise ValueError("seconds must be non-negative")
        if seconds == 0.0:
            return self.current_tick
        if step_size is None:
            step_size = self._configured_sim_step_size()
        step_size = float(step_size)
        if step_size <= 0.0:
            raise ValueError("step_size must be positive")
        step_num = max(1, int(math.ceil(seconds / step_size)))
        self.tick(step_num=step_num, **tick_kwargs)
        return self.current_tick

    def advance_minutes(self, minutes, step_size=None, **tick_kwargs):
        """Advance by at least ``minutes`` using the configured simulation step."""
        minutes = float(minutes)
        if minutes < 0.0:
            raise ValueError("minutes must be non-negative")
        return self.advance_seconds(
            minutes * 60.0,
            step_size=step_size,
            **tick_kwargs,
        )

    def advance_and_snapshot(
            self,
            number_of_ticks=1,
            taxi_ids=None,
            field_mask=None,
            include_details=False,
            future_supply_thresholds=None,
            event_cursor=0,
            command_id=None,
            starting_tick=None,
            timeout_ms=None,
            legacy_fallback=True):
        """Atomically advance and return a compact ride-hailing snapshot.

        The command ID is stable across retries, making the optimized control
        idempotent. Older simulators automatically use ``tick`` followed by
        the established query methods, with a small client-side replay cache.
        """
        number_of_ticks = int(number_of_ticks)
        if number_of_ticks < 1:
            raise ValueError("number_of_ticks must be a positive integer")
        if self.current_tick is None:
            raise RuntimeError("Cannot advance before the simulator reports its tick")
        if starting_tick is None:
            starting_tick = int(self.current_tick)
        else:
            starting_tick = int(starting_tick)
        command_id = str(command_id or uuid.uuid4().hex)
        taxi_ids = [] if taxi_ids is None else _as_list(taxi_ids)
        fields = self._field_mask_set(field_mask, include_details=include_details)
        thresholds = (
            [] if future_supply_thresholds is None
            else _as_list(future_supply_thresholds)
        )
        if timeout_ms is None:
            timeout_ms = max(120000, number_of_ticks * 10000)
        timeout_ms = max(1000, int(timeout_ms))

        if self._supports_feature("advanceAndSnapshot"):
            data = {
                "commandId": command_id,
                "startTick": starting_tick,
                "tickCount": number_of_ticks,
                "taxiIds": taxi_ids,
                "eventCursor": max(0, int(event_cursor)),
                "timeoutMs": timeout_ms,
            }
            if fields:
                data["fieldMask"] = sorted(fields)
            if include_details:
                data["includeDetails"] = True
            if thresholds:
                data["futureSupplyThresholds"] = thresholds
            started_at = time.perf_counter()

            def matching_command(response):
                if (
                        not isinstance(response, dict)
                        or response.get("messageType") != "advanceAndSnapshot"):
                    return True
                response_id = response.get("commandId")
                return response_id is None or str(response_id) == command_id

            response = self.send_receive_msg(
                {"messageType": "advanceAndSnapshot", "data": data},
                ignore_heartbeats=True,
                max_attempts=5,
                timeout=max(float(self.timeout), timeout_ms / 1000.0 + 5.0),
                return_errors=True,
                command_id=command_id,
                response_matcher=matching_command,
            )
            if not self._is_unsupported_response(response):
                if response.get("messageType") != "advanceAndSnapshot":
                    raise RuntimeError(
                        "Expected advanceAndSnapshot, received "
                        f"{response.get('messageType')}"
                    )
                response_command_id = response.get("commandId")
                if response_command_id is None or str(response_command_id) != command_id:
                    raise RuntimeError(
                        "METS-R SIM advanceAndSnapshot response commandId "
                        f"{response_command_id!r} does not match {command_id!r}"
                    )
                if str(response.get("status", "ok")).lower() != "ok":
                    raise RuntimeError(
                        f"METS-R SIM rejected advanceAndSnapshot: {response}"
                    )
                self._update_current_tick_from_message(response)
                if not self._routing_truthy(response.get("replayed", False)):
                    self._record_server_tick_time(started_at, starting_tick)
                return response
            self._feature_support["advanceAndSnapshot"] = False

        if not legacy_fallback:
            raise RuntimeError("Connected METS-R SIM does not support advanceAndSnapshot")
        fingerprint = json.dumps({
            "startTick": starting_tick,
            "tickCount": number_of_ticks,
            "taxiIds": taxi_ids,
            "fieldMask": sorted(fields),
            "thresholds": thresholds,
            "eventCursor": max(0, int(event_cursor)),
        }, sort_keys=True, separators=(",", ":"))
        cached = self._legacy_command_results.get(command_id)
        if cached is not None:
            if cached["fingerprint"] != fingerprint:
                raise ValueError("command_id was already used with a different payload")
            replay = dict(cached["response"])
            replay["replayed"] = True
            return replay
        if int(self.current_tick) != starting_tick:
            raise RuntimeError(
                f"starting_tick {starting_tick} does not match current tick "
                f"{self.current_tick}"
            )

        self.tick(step_num=number_of_ticks)
        snapshot = self.query_ride_hailing_snapshot(
            taxi_ids=taxi_ids,
            field_mask=fields,
            include_details=include_details,
            future_supply_thresholds=thresholds,
            event_cursor=event_cursor,
        )
        response = {
            "messageType": "advanceAndSnapshot",
            "status": "ok",
            "commandId": command_id,
            "startTick": starting_tick,
            "finalTick": int(self.current_tick),
            "advancedTicks": int(self.current_tick) - starting_tick,
            "replayed": False,
            "legacyFallback": True,
            "taxis": snapshot["taxis"],
            "availableTaxiSummary": snapshot["availableTaxiSummary"],
            "futureSupply": snapshot["futureSupply"],
            "events": snapshot["events"],
            "nextEventCursor": snapshot["nextEventCursor"],
        }
        self._legacy_command_results[command_id] = {
            "fingerprint": fingerprint,
            "response": dict(response),
        }
        while len(self._legacy_command_results) > 128:
            self._legacy_command_results.pop(next(iter(self._legacy_command_results)))
        return response

    # save the simulation instance to zip
    def save(self, filename):
        """Save the current simulation state to a ZIP snapshot on the simulator."""
        msg = {"messageType": "save", "data": {"path": filename}}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["messageType"] == "save", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def load(self, filename, reload_network=True):
        """Restore the simulator from a saved snapshot.

        Parameters
        ----------
        filename : str
            Path to the ``.zip`` snapshot previously written by :meth:`save`.
        reload_network : bool
            When ``True`` the simulator fully rebuilds the road
            network and all facilities from the archive.  When ``False`` the
            simulator skips network and facility reconstruction and only
            restores agent state, which is substantially faster when the
            snapshot was taken from the same running instance (e.g. to replay
            an experiment from a preheated checkpoint).  The response includes
            a ``fastLoad`` field confirming whether fast restoration was used.
        """
        msg = {"messageType": "load", "data": {"path": filename, "reloadNetwork": bool(reload_network)}}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "load", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        self._legacy_command_results.clear()
        self._capabilities_cache = None
        if "tick" in res:
            self.current_tick = int(res["tick"])
        else:
            synced_tick = self.query_tick()
            self.current_tick = int(synced_tick)
        return res

    # terminate the simulation
    def terminate(self):
        """End the simulation and close the client connection and visualizations."""
        msg = {"messageType": "end"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "end", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        self.close()
