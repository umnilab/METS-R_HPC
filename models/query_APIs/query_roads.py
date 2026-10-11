"""Query APIs for roads."""

from models._api_utils import _batch_field_values
from utils.primitives import _as_list


class QueryRoadsMixin:
    """Query APIs for roads."""

    def query_active_roads(self):
        """Query active physical roads and occupied intersection connectors.

        Records use ``segmentId`` and ``segmentType``; connectors report
        ``laneIndex=-1``.
        """
        msg = {"messageType": "activeRoads"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "activeRoads", res["messageType"]
        return res

    def query_road(self, id = None):
        """Query static and real-time attributes of one or more roads.

        Without ``id`` returns the full road index::

            {'roadIds': [...], 'connectorIds': [...], 'messageType': 'road'}

        With ``id`` (SUMO road IDs or connector IDs) each matched facility
        produces a record. Physical roads include::

            {
              'segmentId':        <str>   SUMO original road ID,
              'roadType':         <int>   road type code,
              'vehicleCount':     <int>   current number of vehicles on the road,
              'speedLimit':       <float> posted speed limit (m/s),
              'travelTime':       <float> recent mean travel time (s),
              'travelTimeP90':    <float> estimated 90th-percentile travel time (s),
              'travelTimeConfidence': <float> estimator confidence in [0, 1],
              'travelTimeEffectiveSampleCount': <float> effective sample count,
              'travelTimeSampleAgeSeconds': <float> age of the latest sample,
              'travelTimeLiveVehicleCount': <int> live observations,
              'travelTimeStoppedFraction': <float> stopped live-vehicle fraction,
              'travelTimeLiveLowerBound': <float> live lower-bound time (s),
              'travelTimeLiveMeanSpeed': <float> live mean speed (m/s),
              'travelTimeEstimateSource': <str> estimator evidence source,
              'routingWeight':    <float> current routing-graph cost; this can
                                           differ from travelTime after
                                           :meth:`update_road_weights`,
              'length':           <float> road length (m),
              'energyConsumed':   <float> cumulative energy consumed on this road (kWh),
              'downstreamIds':    <list>  downstream road IDs,
              'parkingCapacity':  <int>   parking capacity on this road,
              'parkedVehicleCount': <int> current parked or reserved vehicles
            }

        Connector records use ``segmentType='connector'``, ``laneIndex=-1``,
        source/target road IDs, conflict metadata, and geometry metrics.

        Parameters
        ----------
        id : str | list[str] | None
            Physical road or connector ID(s). Pass ``None`` for the full index.
        """
        my_msg = {"messageType": "road"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "road", res["messageType"]
        return res

    def query_connector_path(self, connectorID, connectorPathID = None):
        """Query lane-to-lane paths for one or more intersection connectors.

        Omitting ``connectorPathID`` returns every path on each connector.
        Supplying a zero-based connector-local path ID returns the exact path,
        including source/target lane indexes, via-lane IDs, internal edge IDs,
        declared geometry, traffic-light metadata, and custom parameters.

        Parameters
        ----------
        connectorID : str | list[str]
            Connector ID(s), normally ``<sourceRoadID>_<targetRoadID>``.
        connectorPathID : int | None | list[int | None]
            Optional connector-local path ID. A scalar is broadcast across a
            connector batch.
        """
        connector_ids = _as_list(connectorID)
        path_ids = _batch_field_values(
            connectorPathID,
            len(connector_ids),
            "connectorPathID",
            batch_name="connectorID",
        )
        data = []
        for connector_id, path_id in zip(connector_ids, path_ids):
            record = {"connectorId": connector_id}
            if path_id is not None:
                record["connectorPathId"] = path_id
            data.append(record)

        msg = {"messageType": "connectorPath", "data": data}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "connectorPath", res["messageType"]
        return res

    def query_entering_vehicle_queue(self, roadID = None):
        """Query vehicles waiting in one or more road entering queues.

        Co-simulation roads hold this queue until the external simulator calls
        :meth:`enter_road_from_queue`. Without ``roadID`` the server returns
        the road index. With road IDs, each record includes ``enteringVehicleIDs``
        and detailed queue entries with visible/private IDs, internal IDs,
        vehicle type, departure tick, and readiness. The index and lookup also
        support connector IDs; connector records set ``isConnector=True`` and
        ``laneIndex=None``.
        """
        msg = {"messageType": "enteringVehicleQueue"}
        if roadID is not None:
            msg["data"] = _as_list(roadID)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "enteringVehicleQueue", res["messageType"]
        return res

    def query_centerline(self, id, lane_index = -1, transform_coords = False):
        """Query the geometric center-line of a road or a specific lane.

        Each matched physical road returns::

            {
              'segmentId':  <str>         SUMO road ID,
              'centerline': [[x, y, z], ...]  ordered coordinate list
            }

        Connector IDs return their representative centerline plus
        ``centerlines`` for all lane-to-lane movements, with
        ``isConnector=True`` and ``laneIndex=None``. The requested ``lane_index``
        is ignored for connectors.

        Parameters
        ----------
        id : str | list[str]
            Physical road or connector ID(s). Required; cannot be ``None``.
        lane_index : int | list[int]
            Index of the lane whose centerline to return.
            Use ``-1`` (default) to get the road's overall start/end points
            instead of a per-lane polyline.
        transform_coords : bool | list[bool]
            ``False`` returns SIM/internal coordinates (WGS-84 lon/lat for
            georeferenced networks). ``True`` returns projected/local SUMO
            coordinates.
        """
        my_msg = {"messageType": "centerLine"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            if not isinstance(lane_index, list):
                lane_index = [lane_index] * len(id)
            if not isinstance(transform_coords, list):
                transform_coords = [transform_coords] * len(id)
            for i, lane_idx, tran in zip(id, lane_index, transform_coords):
                my_msg['data'].append({
                    "segmentId": i,
                    "laneIndex": lane_idx,
                    "transformCoordinates": tran,
                })
        else:
            raise ValueError("id cannot be None for query_centerLine")
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "centerLine", res["messageType"]
        return res

    def query_road_weights(self, roadID = None):
        """Query costs for physical roads or intersection connectors.

        Without ``roadID`` returns all road/edge IDs::

            {'roadIds': [...], 'connectorIds': [...], 'messageType': 'edgeWeight'}

        Physical-road records contain routing-edge costs. Connector records
        instead use their estimated travel time as ``routingWeight`` and include
        connector travel-time and geometry fields with ``laneIndex=-1``.

        With ``roadID`` each matched physical road produces::

            {
              'segmentId':       <str>   SUMO road ID,
              'length':          <float> road length (m),
              'routingWeight':   <float> current edge weight in seconds used
                                         by the router (typically travel time,
                                         may be overridden via
                                         :meth:`update_road_weights`),
              'travelTimeP90':   <float> estimated 90th-percentile time (s),
              'travelTimeConfidence': <float> estimator confidence in [0, 1],
              'travelTimeEffectiveSampleCount': <float> effective samples,
              'travelTimeSampleAgeSeconds': <float> latest-sample age (s),
              'travelTimeLiveVehicleCount': <int> live observations,
              'travelTimeStoppedFraction': <float> stopped observation fraction,
              'travelTimeLiveLowerBound': <float> live lower-bound time (s),
              'travelTimeLiveMeanSpeed': <float> live mean speed (m/s),
              'travelTimeEstimateSource': <str> estimator evidence source,
              'segmentType':     'road'
            }

        Parameters
        ----------
        roadID : str | list[str] | None
            Physical road or connector ID(s). Pass ``None`` to get the full
            queryable facility index.
        """
        msg = {"messageType": "edgeWeight"}
        if roadID is not None:
            msg["data"] = []
            if not isinstance(roadID, list):
                roadID = [roadID]
            for i in roadID:
                msg["data"].append(i)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "edgeWeight", res["messageType"]
        return res
