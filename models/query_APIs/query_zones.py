"""Query APIs for zones."""




class QueryZonesMixin:
    """Query APIs for zones."""

    def query_zone(self, id = None):
        """Query demand and supply statistics for one or more traffic zones.

        Without ``id`` returns the full zone index::

            {'zoneIds': [...], 'messageType': 'zone'}

        With ``id`` each matched zone produces::

            {
              'zoneId':      <int>   zone ID,
              'zoneType':    <int>   zone type code,
              'taxiDemand':  <int>   number of pending taxi requests currently
                                     waiting in this zone,
              'busDemand':   <int>   number of pending bus requests currently
                                     waiting in this zone,
              'vehicleStock': <int>  number of available taxis parked / cruising
                                     in this zone at this tick,
              'x':           <float> centroid x coordinate (network CRS),
              'y':           <float> centroid y coordinate (network CRS),
              'z':           <float> centroid elevation,
              'leftTaxiRequests':    <int> cumulative taxi requests that
                                           abandoned after waiting too long,
              'leftTaxiPassengers':  <int> cumulative passengers in those
                                           abandoned taxi requests,
              'leftBusRequests':     <int> cumulative bus requests that
                                           abandoned after waiting too long,
              'leftBusPassengers':   <int> cumulative passengers in those
                                           abandoned bus requests
            }

        Parameters
        ----------
        id : int | list[int] | None
            Zone ID(s) to query. Pass ``None`` to get the full zone ID list.
        """
        my_msg = {"messageType": "zone"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "zone", res["messageType"]
        return res
