"""Query APIs for public vehicles."""




class QueryPublicVehiclesMixin:
    """Query APIs for public vehicles."""

    def query_taxi(self, id = None):
        """Query the state of one or more e-taxis.

        Without ``id`` returns a fleet-level summary::

            {'taxiIds': [...], 'messageType': 'taxi'}

        With ``id`` each matched taxi produces::

            {
              'taxiId':   <int>   internal vehicle ID,
              'state':    <int>   operational state:
                                   0 = PARKING        â€“ parked at a zone, waiting for a request
                                   1 = OCCUPIED_TRIP  â€“ carrying passenger(s) to drop-off
                                   2 = INACCESSIBLE_RELOCATION_TRIP â€“ repositioning, not assignable
                                   4 = CHARGING_TRIP  â€“ heading to a charging station
                                   5 = CRUISING_TRIP  â€“ cruising without a passenger
                                   6 = PICKUP_TRIP    â€“ en-route to pick up a passenger
                                   7 = ACCESSIBLE_RELOCATION_TRIP â€“ repositioning but still assignable
                                   -1 = NONE_OF_THE_ABOVE â€“ not on network, idle, or in an unrecognized state
              'x':        <float> SIM/internal x coordinate,
              'y':        <float> SIM/internal y coordinate,
              'z':        <float> elevation,
              'originZoneId': <int> current origin zone ID,
              'destinationZoneId': <int> current destination zone ID
                                  (negative â†’ heading to a charging station),
              'passengerCount': <int> number of passengers currently on board,
              'remainingDistance': <float> connector-inclusive remaining
                                           active-trip distance in meters,
              'remainingDistanceMiles': <float> remaining active-trip distance in miles,
              'remainingConnectorDistance': <float> connector-only remainder (m),
              'remainingConnectorTravelTime': <float> connector-only remainder (s),
              'currentParkingRoadId': <str | None> original road ID where the
                                      taxi is parked or has reserved parking,
              'segmentId': <str>   physical or connector road ID,
              'onConnector': <bool>,
              'laneIndex':    <int | None> compact lane index, or None on a connector
            }

        Parameters
        ----------
        id : int | list[int] | None
            Taxi ID(s) to query. Pass ``None`` to get the full fleet ID list.
        """
        my_msg = {"messageType": "taxi"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)

        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "taxi", res["messageType"]
        return res

    def query_available_taxis(self, zoneID = None):
        """Query taxis currently available for dispatch.

        Without ``zoneID`` returns available taxis across all zones. With a
        zone ID, returns only the available-taxi pool for that zone.

        Each entry in ``data`` includes ``taxiId``, ``zoneId``, state,
        position, battery level, battery feasibility, and passenger count.
        """
        msg = {"messageType": "availableTaxis"}
        if zoneID is not None:
            msg["data"] = zoneID
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "availableTaxis", res["messageType"]
        return res

    def query_almost_finished_taxis(
            self,
            distance_threshold_miles = None,
            distance_threshold_meters = None,
            zoneID = None):
        """Query occupied taxis expected to become available soon.

        The simulator filters to occupied taxis with one onboard request, no
        queued pickup requests, and remaining trip distance below the supplied
        threshold. A ``zoneID`` filter selects the request destination zone.
        """
        msg = {"messageType": "almostFinishedTaxis"}
        params = {}
        if distance_threshold_meters is not None:
            params["distanceThresholdMeters"] = distance_threshold_meters
        elif distance_threshold_miles is not None:
            params["distanceThresholdMiles"] = distance_threshold_miles
        if zoneID is not None:
            params["zoneId"] = zoneID
        if params:
            msg["data"] = params
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "almostFinishedTaxis", res["messageType"]
        return res

    def query_bus(self, id = None):
        """Query the state of one or more electric buses.

        Without ``id`` returns::

            {'busIds': [...], 'messageType': 'bus'}

        With ``id`` each matched bus produces::

            {
              'busId':         <int>   internal vehicle ID,
              'routeName':     <str>   name of the current bus route
                                       (empty string if the bus is idle),
              'stopZoneIds':   <list>  zone IDs in the assigned stop sequence,
              'currentStopIndex': <int> index of the last completed stop
                                       in the route's stop list (0-based),
              'passengerCount': <int>  number of passengers currently on board,
              'battery':       <float> remaining battery energy (kWh)
            }

        Notes
        -----
        Bus trip states are the same integer codes as other vehicles
        (see :meth:`query_vehicle`), but buses only use:
          ``BUS_TRIP (3)``, ``CHARGING_TRIP (4)``, and
          ``CHARGING_RETURN_TRIP (9)`` during normal operation.

        Parameters
        ----------
        id : int | list[int] | None
            Bus ID(s) to query. Pass ``None`` to get the full fleet ID list.
        """
        my_msg = {"messageType": "bus"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "bus", res["messageType"]
        return res

    def query_bus_route(self, routeID = None):
        """Query the stop sequence and road IDs of one or more bus routes.

        Without ``routeID`` returns all route identifiers::

            {'routeIds': [...], 'routeNames': [...], 'messageType': 'busRoute'}

        With ``routeID`` (route *name* strings) each matched route produces::

            {
              'routeName': <str>   human-readable route name,
              'routeId':   <int>   internal integer route ID,
              'stopZoneIds': <list> ordered list of zone IDs the bus visits,
              'stopRoadIds': <list> corresponding road IDs at each stop
            }

        Parameters
        ----------
        routeID : str | list[str] | None
            Route name(s) to query. Pass ``None`` to list all routes.
        """
        msg = {"messageType": "busRoute"}
        if routeID is not None:
            msg["data"] = []
            if not isinstance(routeID, list):
                routeID = [routeID]
            for i in routeID:
                msg["data"].append(i)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "busRoute", res["messageType"]
        return res

    def query_route_bus(self, routeID = None):
        """Query the IDs of all buses currently operating on a given route.

        Without ``routeID`` returns all route identifiers::

            {'routeIds': [...], 'routeNames': [...], 'messageType': 'busWithRoute'}

        With ``routeID`` (route *name* strings) each matched route produces::

            {
              'routeName': <str>   human-readable route name,
              'routeId':   <int>   internal integer route ID,
              'busIds':    <list>  IDs of buses currently assigned to this route
            }

        Parameters
        ----------
        routeID : str | list[str] | None
            Route name(s) to query. Pass ``None`` to list all routes.
        """
        msg = {"messageType": "busWithRoute"}
        if routeID is not None:
            msg["data"] = []
            if not isinstance(routeID, list):
                routeID = [routeID]
            for i in routeID:
                msg["data"].append(i)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "busWithRoute", res["messageType"]
        return res
