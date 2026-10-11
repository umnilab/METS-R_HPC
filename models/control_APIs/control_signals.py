"""Control APIs for signals."""




class ControlSignalsMixin:
    """Control APIs for signals."""

    # Traffic signal phase control
    # Update the signal phase given signal ID and target phase (optionally with phase time offset)
    # If only phase is provided, starts from the beginning of that phase (phaseTime = 0)
    def update_signal(self, signalID, targetPhase, phaseTime = None):
        """Set traffic signal phases with optional offsets in seconds."""
        msg = {"messageType": "updateSignal", "data": []}
        if not isinstance(signalID, list):
            signalID = [signalID]
            targetPhase = [targetPhase]
        if not isinstance(targetPhase, list):
            targetPhase = [targetPhase] * len(signalID)
        if phaseTime is None:
            phaseTime = [None] * len(signalID)
        elif not isinstance(phaseTime, list):
            phaseTime = [phaseTime] * len(signalID)
        else:
            # If phaseTime is a list, ensure it matches the length
            if len(phaseTime) != len(signalID):
                phaseTime = phaseTime * (len(signalID) // len(phaseTime) + 1)
                phaseTime = phaseTime[:len(signalID)]

        assert len(signalID) == len(targetPhase) == len(phaseTime), "Length of signalID, targetPhase, and phaseTime must be the same"

        for sig_id, tgt_phase, ph_time in zip(signalID, targetPhase, phaseTime):
            signal_data = {"signalId": sig_id, "phase": tgt_phase}
            if ph_time is not None:
                signal_data["phaseOffsetSeconds"] = ph_time
            msg["data"].append(signal_data)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateSignal", res["messageType"]
        return res

    # Update signal phase timing (green, yellow, red durations)
    def update_signal_timing(self, signalID, greenTime, yellowTime, redTime):
        """Set green, yellow, and red traffic signal durations in seconds."""
        msg = {"messageType": "updateSignalTiming", "data": []}
        if not isinstance(signalID, list):
            signalID = [signalID]
            greenTime = [greenTime]
            yellowTime = [yellowTime]
            redTime = [redTime]
        if not isinstance(greenTime, list):
            greenTime = [greenTime] * len(signalID)
        if not isinstance(yellowTime, list):
            yellowTime = [yellowTime] * len(signalID)
        if not isinstance(redTime, list):
            redTime = [redTime] * len(signalID)

        assert len(signalID) == len(greenTime) == len(yellowTime) == len(redTime), "Length of signalID, greenTime, yellowTime, and redTime must be the same"

        for sig_id, green, yellow, red in zip(signalID, greenTime, yellowTime, redTime):
            msg["data"].append({"signalId": sig_id, "greenSeconds": green, "yellowSeconds": yellow, "redSeconds": red})

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateSignalTiming", res["messageType"]
        return res

    # Set a complete new phase plan for a signal (phase timing + starting state + offset)
    # Time values are in seconds
    def set_signal_phase_plan(self, signalID, greenTime, yellowTime, redTime, startPhase, phaseOffset = None):
        """Set signal phase durations, starting phases, and offsets in seconds."""
        msg = {"messageType": "setSignalPhasePlan", "data": []}
        if not isinstance(signalID, list):
            signalID = [signalID]
            greenTime = [greenTime]
            yellowTime = [yellowTime]
            redTime = [redTime]
            startPhase = [startPhase]
        if not isinstance(greenTime, list):
            greenTime = [greenTime] * len(signalID)
        if not isinstance(yellowTime, list):
            yellowTime = [yellowTime] * len(signalID)
        if not isinstance(redTime, list):
            redTime = [redTime] * len(signalID)
        if not isinstance(startPhase, list):
            startPhase = [startPhase] * len(signalID)
        if phaseOffset is None:
            phaseOffset = [None] * len(signalID)
        elif not isinstance(phaseOffset, list):
            phaseOffset = [phaseOffset] * len(signalID)
        else:
            # If phaseOffset is a list, ensure it matches the length
            if len(phaseOffset) != len(signalID):
                phaseOffset = phaseOffset * (len(signalID) // len(phaseOffset) + 1)
                phaseOffset = phaseOffset[:len(signalID)]

        assert len(signalID) == len(greenTime) == len(yellowTime) == len(redTime) == len(startPhase) == len(phaseOffset), "Length of all parameters must match"

        for sig_id, green, yellow, red, start_phase, ph_offset in zip(signalID, greenTime, yellowTime, redTime, startPhase, phaseOffset):
            signal_data = {"signalId": sig_id, "greenSeconds": green, "yellowSeconds": yellow, "redSeconds": red, "startPhase": start_phase}
            if ph_offset is not None:
                signal_data["phaseOffsetSeconds"] = ph_offset
            msg["data"].append(signal_data)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "setSignalPhasePlan", res["messageType"]
        return res

    # Set a complete new phase plan with tick-level precision
    # Time values are in simulation ticks for more precise control
    def set_signal_phase_plan_ticks(self, signalID, greenTicks, yellowTicks, redTicks, startPhase, tickOffset = None):
        """Set signal phase durations, starting phases, and offsets in ticks."""
        msg = {"messageType": "setSignalPhasePlanTicks", "data": []}
        if not isinstance(signalID, list):
            signalID = [signalID]
            greenTicks = [greenTicks]
            yellowTicks = [yellowTicks]
            redTicks = [redTicks]
            startPhase = [startPhase]
        if not isinstance(greenTicks, list):
            greenTicks = [greenTicks] * len(signalID)
        if not isinstance(yellowTicks, list):
            yellowTicks = [yellowTicks] * len(signalID)
        if not isinstance(redTicks, list):
            redTicks = [redTicks] * len(signalID)
        if not isinstance(startPhase, list):
            startPhase = [startPhase] * len(signalID)
        if tickOffset is None:
            tickOffset = [None] * len(signalID)
        elif not isinstance(tickOffset, list):
            tickOffset = [tickOffset] * len(signalID)
        else:
            # If tickOffset is a list, ensure it matches the length
            if len(tickOffset) != len(signalID):
                tickOffset = tickOffset * (len(signalID) // len(tickOffset) + 1)
                tickOffset = tickOffset[:len(signalID)]

        assert len(signalID) == len(greenTicks) == len(yellowTicks) == len(redTicks) == len(startPhase) == len(tickOffset), "Length of all parameters must match"

        for sig_id, green, yellow, red, start_phase, tck_offset in zip(signalID, greenTicks, yellowTicks, redTicks, startPhase, tickOffset):
            signal_data = {"signalId": sig_id, "greenTicks": green, "yellowTicks": yellow, "redTicks": red, "startPhase": start_phase}
            if tck_offset is not None:
                signal_data["phaseOffsetTicks"] = tck_offset
            msg["data"].append(signal_data)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "setSignalPhasePlanTicks", res["messageType"]
        return res
