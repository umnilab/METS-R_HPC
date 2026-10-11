"""Query APIs for charging stations."""




class QueryChargingStationsMixin:
    """Query APIs for charging stations."""

    def query_chargingStation(self, id = None):
        """Query the status and capacity of one or more charging stations.

        Without ``id`` returns the full station index::

            {'chargingStationIds': [...], 'messageType': 'chargingStation'}

        With ``id`` each matched station produces::

            {
              'chargingStationId': <int>   internal station ID,
              'level2ChargerCount': <int>  total number of L2 charger ports,
              'level3ChargerCount': <int>  total number of fast charger ports,
              'level2Price':       <float> current L2 price per kWh,
              'level3Price':       <float> current L3 price per kWh,
              'busChargerCount':   <int>   bus-dedicated charger ports,
              'availableLevel2ChargerCount': <int> available L2 ports,
              'availableLevel3ChargerCount': <int> available L3 ports,
              'x':                 <float> station x coordinate (network CRS),
              'y':                 <float> station y coordinate (network CRS),
              'z':                 <float> elevation
            }

        Notes
        -----
        Charger type codes used internally by the server:
          ``ChargingStation.L2 = 0``  (AC Level-2),
          ``ChargingStation.L3 = 1``  (DC fast charging),
          ``ChargingStation.BUS = 2`` (depot charger for electric buses).

        Parameters
        ----------
        id : int | list[int] | None
            Charging station ID(s) to query. Pass ``None`` to get the full list.
        """
        my_msg = {"messageType": "chargingStation"}
        if id is not None:
            my_msg['data'] = []
            if not isinstance(id, list):
                id = [id]
            for i in id:
                my_msg['data'].append(i)
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["messageType"] == "chargingStation", res["messageType"]
        return res
