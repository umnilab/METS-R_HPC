"""Query APIs for cosim."""

import math


class QueryCosimMixin:
    """Query APIs for cosim."""

    def query_cosim_entering_vehicle_queue(self):
        """Query entering queues for every road currently marked as co-sim."""
        msg = {"messageType": "coSimEnteringVehicleQueue"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "coSimEnteringVehicleQueue", res["messageType"]
        return res

    def query_cosim_vehicle(self):
        """Query vehicles currently owned by the external co-simulator.

        Returns vehicles currently owned by the external co-simulator on roads
        and connectors registered through :meth:`set_cosim_road`. Each entry in
        ``data`` represents one vehicle::

            {
              'vehicleId': <int>   vehicle ID
                                    â€“ for private vehicles (EV/GV) this is the
                                      *external* private-vehicle ID
                                    â€“ for public vehicles (taxi/bus) this is the
                                      internal simulation ID,
              'isPrivate': <bool>  True  â†’ private vehicle (EV / GV)
                                   False â†’ public vehicle (taxi / bus),
              'coordinateTrail': <list> recent coordinate history (up to 6 entries),
                                   each entry is [x, y, z, bearing, speed],
              'routeRoadIds': <list> list of upcoming road IDs in the vehicle's
                                   current planned route,
              'segmentId': <str>   physical or connector segment ID,
              'connectorId': <str> present while on a connector,
              'onConnector': <bool>,
              'laneIndex':    <int> compact lane index, or None on a connector
            }

        Connector occupancy ends when the vehicle leaves that segment. Native
        vehicles just beyond a controlled connector are returned separately by
        :meth:`query_boundary_vehicle` and remain under METS-R control.

        Returns
        -------
        dict
            ``{'data': [...], 'messageType': 'coSimVehicle'}``
        """
        my_msg = {"messageType": "coSimVehicle"}
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "coSimVehicle", res["messageType"]
        return res

    # Historical camel-case spelling retained for source compatibility.
    query_coSimVehicle = query_cosim_vehicle

    def query_boundary_vehicle(self, boundary_dist=6.0):
        """Query native vehicles near exits of co-simulation connectors.

        Sends ``boundaryVehicle`` (the simulator's ``queryBoundaryVeh`` handler)
        with the required top-level ``boundaryDist`` in meters. Each entry in
        ``data`` uses the same bridge schema as :meth:`query_cosim_vehicle`:
        ``vehicleId``, ``isPrivate``, ``coordinateTrail``, ``routeRoadIds``, and
        road/lane/control fields.

        These vehicles are on native physical roads immediately downstream of
        controlled connectors, at a non-negative distance strictly less than
        ``boundary_dist`` from their current lane's entry. The cutoff is fixed
        in meters, independent of vehicle length. ``controlMode`` is ``native``
        and ``onConnector`` is false. Mirror them as obstacles in the external
        simulator; they remain METS-R controlled.
        Use :meth:`query_vehicle` with the returned IDs and private flags to
        obtain current poses in the desired coordinate system.

        Parameters
        ----------
        boundary_dist : float, default 6.0
            Finite, non-negative entry-distance cutoff in meters. Zero returns
            no vehicles, including those exactly at entry. The 6 m default is
            supplied by this client to support existing no-argument calls;
            the server requires an explicit ``boundaryDist``. This replaces
            the old server rule of 1.2 times each vehicle's length.

        Returns
        -------
        dict
            ``{'messageType': 'boundaryVehicle', 'data': [...]}``, with an empty
            list when there are no qualifying vehicles.

        Raises
        ------
        ValueError
            If ``boundary_dist`` is negative, non-finite, boolean, or cannot be
            converted to a number. No request is sent for invalid input.
        """
        error = "boundary_dist must be a finite, non-negative distance in meters"
        if isinstance(boundary_dist, bool):
            raise ValueError(error)
        try:
            boundary_dist = float(boundary_dist)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(error) from exc
        if not math.isfinite(boundary_dist) or boundary_dist < 0.0:
            raise ValueError(error)
        msg = {"messageType": "boundaryVehicle", "boundaryDist": boundary_dist}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "boundaryVehicle", res["messageType"]
        return res

    query_boundaryVehicle = query_boundary_vehicle

    queryBoundaryVeh = query_boundary_vehicle

    def query_cosim_roads(self):
        """Query every physical road and connector controlled by co-simulation.

        The response contains ``roadIds``, ``connectorIds``, and full ``data``
        records using ``segmentId``, ``segmentType``, ``controlMode``, and
        canonical connector source/target and geometry fields.
        """
        msg = {"messageType": "coSimRoad"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "coSimRoad", res["messageType"]
        return res
