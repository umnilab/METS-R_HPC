"""Query APIs for signals."""




class QuerySignalsMixin:
    """Query APIs for signals."""

    def query_signal(self, id = None):
        """Query the phase state of one or more traffic signals.

        Without ``id`` returns the full signal index::

            {'signalIds': [...], 'messageType': 'signal'}

        With ``id`` each matched signal produces::

            {
              'signalId':         <int>  internal signal ID,
              'signalGroupId':          <str>  SUMO junction ID this signal belongs to,
              'state':            <int>  current phase:
                                           0 = Green
                                           1 = Yellow
                                           2 = Red,
              'nextState':        <int>  next phase (same encoding),
              'nextUpdateTime':   <int>  simulation tick at which the phase will change,
              'phaseTicks':       <list> [green_ticks, yellow_ticks, red_ticks]
                                         duration of each phase in simulation ticks
            }

        Notes
        -----
        Phase cycle order is always Green â†’ Yellow â†’ Red â†’ Green.
        Each tick corresponds to ``GlobalVariables.SIMULATION_STEP_SIZE`` seconds
        (typically 0.5 s, so multiply ticks Ã— 0.5 to get seconds).

        Use :meth:`query_signal_group` to map a SUMO junction ID to the list of
        individual signal IDs it contains.

        Parameters
        ----------
        id : int | list[int] | None
            Signal ID(s) to query. Pass ``None`` to get the full signal ID list.
        """
        my_msg = {"messageType": "signal"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "signal", res["messageType"]
        return res

    def query_signal_group(self, id = None):
        """Query the set of signal IDs that belong to each signal group (junction).

        Without ``id`` returns all group IDs::

            {'signalGroupIds': [...], 'messageType': 'signalGroup'}

        With ``id`` (SUMO junction / group IDs) each group produces::

            {
              'signalGroupId':   <str>   SUMO junction ID,
              'signalIds': <list>  list of individual signal IDs in this group
            }

        Parameters
        ----------
        id : str | list[str] | None
            Group / junction ID(s) to query. Pass ``None`` to list all groups.
        """
        my_msg = {"messageType": "signalGroup"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "signalGroup", res["messageType"]
        return res

    def query_signal_between_roads(self, upstream_road, downstream_road):
        """Query the signal controlling the connection between two consecutive roads.

        Each connection produces::

            {
              'upstreamRoadId':  <str>  SUMO ID of the upstream road,
              'downstreamRoadId':<str>  SUMO ID of the downstream road,
              'signalId':        <int>  signal ID (use with :meth:`query_signal`),
              'state':           <int>  current phase (0=Green, 1=Yellow, 2=Red),
              'nextState':       <int>  next phase (same encoding),
              'nextUpdateTick':  <int>  simulation tick at which the phase changes,
              'phaseTicks':      <list> [green_ticks, yellow_ticks, red_ticks],
              'junctionId':      <int>  internal junction ID,
              'status':          <str>  'ok' or 'error' (road / junction not found)
            }

        Parameters
        ----------
        upstream_road : str | list[str]
            SUMO road ID(s) of the upstream road.
        downstream_road : str | list[str]
            SUMO road ID(s) of the downstream road (matched pairwise).
        """
        msg = {"messageType": "signalForConnection", "data": []}
        if not isinstance(upstream_road, list):
            upstream_road = [upstream_road]
        if not isinstance(downstream_road, list):
            downstream_road = [downstream_road] * len(upstream_road)
        assert len(upstream_road) == len(downstream_road), "Length of upstream_road and downstream_road must be the same"

        for up_road, down_road in zip(upstream_road, downstream_road):
            msg["data"].append({"upstreamRoadId": up_road, "downstreamRoadId": down_road})

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "signalForConnection", res["messageType"]
        return res
