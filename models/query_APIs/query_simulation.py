"""Query APIs for simulation."""




class QuerySimulationMixin:
    """Query APIs for simulation."""

    def _apply_tick_response(self, res):
        if res.get("messageType") != "tick":
            raise RuntimeError(f"Expected tick, received {res.get('messageType')}")
        if res.get("status", "ok") != "ok":
            raise RuntimeError(f"METS-R SIM rejected tick query: {res}")
        if "tick" not in res:
            raise RuntimeError(f"METS-R SIM query tick response is missing tick: {res}")
        self._update_current_tick_from_message(res)
        return int(res["tick"])

    def _query_tick_locked(self, timeout = None):
        """Query server tick while self.lock is already held."""
        before_tick = self.current_tick
        self.send_msg({"messageType": "tick"})
        res = self.receive_msg(
            ignore_heartbeats=True,
            waiting_forever=False,
            print_timeout=False,
            timeout=min(5, max(0.1, self.timeout if timeout is None else timeout)),
        )
        if res is None:
            if before_tick != self.current_tick:
                return int(self.current_tick)
            return None
        return self._apply_tick_response(res)

    def query_tick(self):
        """Return the current simulation tick reported by METS-R SIM."""
        res = self.send_receive_msg({"messageType": "tick"}, ignore_heartbeats=True)
        return self._apply_tick_response(res)

    @staticmethod
    def _is_unsupported_response(response):
        if response is None or not isinstance(response, dict):
            return True
        if response.get("messageType") == "error":
            return True
        if str(response.get("status", "ok")).lower() != "error":
            return False
        detail = " ".join(
            str(response.get(key, ""))
            for key in ("message", "errorCode")
        ).lower()
        return any(
            marker in detail
            for marker in ("unknown", "unsupported", "unrecognized", "not found")
        )

    def query_capabilities(self, refresh=False):
        """Return capabilities reported by the current METS-R SIM API."""
        if self._capabilities_cache is not None and not refresh:
            return dict(self._capabilities_cache)
        response = self.send_receive_msg(
            {"messageType": "capabilities"},
            ignore_heartbeats=True,
            max_attempts=1,
            timeout=min(2.0, max(0.1, float(self.timeout))),
            return_errors=True,
        )
        if not isinstance(response, dict):
            raise RuntimeError("METS-R SIM returned no capabilities response")
        if response.get("messageType") != "capabilities":
            raise RuntimeError(
                "Expected capabilities, received "
                + str(response.get("messageType"))
            )
        if response.get("status") != "ok":
            raise RuntimeError(f"METS-R SIM rejected capabilities query: {response}")
        self._capabilities_cache = dict(response)
        self._optimized_api_available = True
        self._feature_support.update({
            name: (
                self._routing_truthy(response[name])
                if name in response else True
            )
            for name in ("advanceAndSnapshot", "routingTopology", "fieldMasks")
        })
        return dict(response)

    def _supports_feature(self, name):
        if name not in self._feature_support:
            self.query_capabilities()
        return bool(self._feature_support.get(name, False))

    def query_tick_status(self):
        """Return server-side stepping status.

        Recent METS-R SIM versions include active-road stepping fields such as
        ``activeRoadStepping`` and ``activeRoadCount`` when that scheduler mode
        is enabled.
        """
        res = self.send_receive_msg({"messageType": "stepStatus"}, ignore_heartbeats=True)
        return res
