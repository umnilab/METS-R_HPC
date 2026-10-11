"""Query APIs for ride hailing."""

from utils.primitives import _as_list


class QueryRideHailingMixin:
    """Query APIs for ride hailing."""

    @staticmethod
    def _field_mask_set(field_mask, include_details=False):
        if include_details:
            return {"*"}
        if field_mask is None:
            return set()
        if isinstance(field_mask, str):
            return {
                field.strip()
                for field in field_mask.split(",")
                if field.strip()
            }
        return {str(field) for field in field_mask}

    def query_ride_hailing_snapshot(
            self,
            taxi_ids=None,
            field_mask=None,
            include_details=False,
            future_supply_thresholds=None,
            event_cursor=0):
        """Return a non-advancing, field-masked ride-hailing snapshot.

        This method uses established query calls on every simulator version.
        :meth:`advance_and_snapshot` uses the fused optimized endpoint when it
        is available and delegates here only for its legacy fallback.
        """
        fields = self._field_mask_set(field_mask, include_details=include_details)
        if taxi_ids is None:
            fleet = self.query_taxi()
            taxi_ids = fleet.get("taxiIds", []) or []
        elif not isinstance(taxi_ids, list):
            taxi_ids = [taxi_ids]
        else:
            taxi_ids = list(taxi_ids)

        if taxi_ids:
            taxi_response = self.query_taxi(taxi_ids)
            raw_records = taxi_response.get("data", [])
        else:
            raw_records = []
        taxis = []
        masked_fields = (
            ("state", ("state",)),
            ("coordinates", ("x", "y", "z")),
            ("currentZoneId", ("currentZoneId",)),
            ("destinationZoneId", ("destinationZoneId",)),
            ("remainingDistance", ("remainingDistance",)),
            ("requestIds", ("toBoardRequestIds", "onBoardRequestIds")),
        )
        for index, taxi_id in enumerate(taxi_ids):
            source = raw_records[index] if index < len(raw_records) else None
            if not isinstance(source, dict):
                taxis.append({"taxiId": taxi_id, "status": "error"})
                continue
            record = {
                "taxiId": source.get("taxiId", taxi_id),
                "status": source.get("status", "ok"),
            }
            for field, names in masked_fields:
                if "*" in fields or field in fields:
                    for name in names:
                        if name not in source:
                            continue
                        if name in {"currentZoneId", "destinationZoneId"} and source[name] is None:
                            continue
                        record[name] = source[name]
            taxis.append(record)

        partial_warnings = []
        available_summary = {}
        try:
            available = self.query_available_taxis()
            for record in available.get("data", []):
                if not isinstance(record, dict):
                    continue
                zone_id = record.get("zoneId")
                if zone_id is not None:
                    key = str(zone_id)
                    available_summary[key] = available_summary.get(key, 0) + 1
        except AssertionError:
            partial_warnings.append(
                "availableTaxiSummary is unavailable from this simulator"
            )

        future_supply = []
        thresholds = [] if future_supply_thresholds is None else _as_list(
            future_supply_thresholds
        )
        if thresholds:
            try:
                zone_index = self.query_zone()
                zone_ids = zone_index.get("zoneIds", []) or []
                zone_response = self.query_zone(id=zone_ids) if zone_ids else {"data": []}
                zone_records = [
                    record
                    for record in zone_response.get("data", [])
                    if isinstance(record, dict)
                ]
                for threshold in thresholds:
                    threshold_value = int(threshold)
                    selected = [
                        record.get("zoneId")
                        for record in zone_records
                        if record.get("zoneId") is not None
                        and record.get("futureSupply") is not None
                        and int(record["futureSupply"]) <= threshold_value
                    ]
                    selected.sort()
                    future_supply.append({
                        "threshold": threshold_value,
                        "count": len(selected),
                        "zoneIdsAtOrBelow": selected,
                    })
            except AssertionError:
                future_supply = []
                partial_warnings.append(
                    "futureSupply is unavailable from this simulator"
                )

        response = {
            "messageType": "rideHailingSnapshot",
            "status": "ok",
            "tick": self.current_tick,
            "taxis": taxis,
            "availableTaxiSummary": available_summary,
            "futureSupply": future_supply,
            "events": [],
            "nextEventCursor": max(0, int(event_cursor)),
            "legacyFallback": True,
        }
        if partial_warnings:
            response["partial"] = True
            response["warnings"] = partial_warnings
        return response

    ride_hailing_snapshot = query_ride_hailing_snapshot
