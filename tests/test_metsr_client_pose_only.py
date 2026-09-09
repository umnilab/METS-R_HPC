"""Wire contract for unconstrained CARLA-authoritative pose publication."""
import pytest

from clients.METSRClient import METSRClient


def capture_client(response=None):
    client = object.__new__(METSRClient)
    sent = []
    if response is None:
        response = {"messageType": "teleportCoSimVeh", "status": "ok", "data": []}

    def send(message, ignore_heartbeats, **kwargs):
        sent.append((message, ignore_heartbeats))
        return response

    client.send_receive_msg = send
    return client, sent


def test_pose_only_single_preserves_unmatched_authority_acknowledgement():
    response = {
        "messageType": "teleportCoSimVeh", "status": "ok", "data": [{
            "vehicleId": 7, "status": "ok", "controlMode": "cosim",
            "releasedFromCoSim": False, "externalPoseAuthoritative": True,
            "shadowMatched": False, "segmentId": None, "laneIndex": None,
            "connectorPathId": None,
        }],
    }
    client, sent = capture_client(response)

    actual = client.teleport_cosim_vehicle(
        7, 1234.5, -987.6, 271.0, z=14.0, speed=8.5,
        private_veh=True, transform_coords=True, pose_only=True,
    )

    assert actual is response
    assert sent == [({"messageType": "teleportCoSimVeh", "data": [{
        "vehicleId": 7, "x": 1234.5, "y": -987.6, "z": 14.0,
        "bearing": 271.0, "speed": 8.5, "isPrivate": True,
        "transformCoordinates": True, "poseOnly": True,
    }]}, True)]


@pytest.mark.parametrize("flags,expected", [(True, [True, True]), ([True, False], [True, False])])
def test_pose_only_batch_broadcast_and_mixed_legacy_records(flags, expected):
    client, sent = capture_client()
    segments = [None, "-51"] if isinstance(flags, list) else None
    lanes = [None, 2] if isinstance(flags, list) else None

    client.teleport_cosim_vehicle(
        [7, 8], [1.0, 2.0], [3.0, 4.0], [90.0, 180.0],
        z=[.1, .2], speed=[5.0, 6.0], private_veh=True,
        pose_only=flags, segment_id=segments, lane_index=lanes,
    )

    records = sent[0][0]["data"]
    assert [record.get("poseOnly", False) for record in records] == expected
    assert "segmentId" not in records[0]
    assert "laneIndex" not in records[0]
    if isinstance(flags, list):
        assert "poseOnly" not in records[1]
        assert records[1]["segmentId"] == "-51"
        assert records[1]["laneIndex"] == 2


def test_false_pose_only_preserves_legacy_alias_payload_exactly():
    client, sent = capture_client()
    arguments = dict(vehID=7, x=1, y=2, bearing=90, observedRoadID="-51", observedLaneID=2)
    client.teleport_cosim_vehicle(**arguments)
    client.teleport_cosim_vehicle(**arguments, pose_only=False)
    assert sent[0] == sent[1]
    assert "poseOnly" not in sent[1][0]["data"][0]


@pytest.mark.parametrize("selector", [
    "segment_id", "segmentID", "road_id", "roadID", "observed_road_id", "observedRoadID",
    "lane_index", "laneIndex", "observed_lane_id", "observedLaneID",
    "connector_path_id", "connectorPathID",
])
def test_pose_only_rejects_every_mapping_selector_alias_before_sending(selector):
    client, sent = capture_client()
    with pytest.raises(ValueError, match="pose_only cannot be combined"):
        client.teleport_cosim_vehicle(7, 1, 2, 90, pose_only=True, **{selector: 1})
    assert not sent


@pytest.mark.parametrize("flags", [None, 1, "true", [True, 0], [True], [True, True, True]])
def test_pose_only_rejects_non_boolean_or_wrong_length_batch(flags):
    client, sent = capture_client()
    with pytest.raises(ValueError, match="pose_only"):
        client.teleport_cosim_vehicle([7, 8], [1, 2], [3, 4], 90, pose_only=flags)
    assert not sent


@pytest.mark.parametrize("field", ["x", "y", "z", "bearing", "speed"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), None, True])
def test_pose_only_rejects_nonfinite_pose_components_without_partial_send(field, value):
    client, sent = capture_client()
    arguments = dict(vehID=[7, 8], x=[1, 2], y=[3, 4], z=0, bearing=90, speed=5, pose_only=True)
    arguments[field] = [0, value]
    with pytest.raises(ValueError, match=f"pose_only {field} must be finite"):
        client.teleport_cosim_vehicle(**arguments)
    assert not sent


def test_mixed_batch_selector_conflict_does_not_send_earlier_valid_record():
    client, sent = capture_client()
    with pytest.raises(ValueError, match="pose_only cannot be combined"):
        client.teleport_cosim_vehicle(
            [7, 8], [1, 2], [3, 4], 90,
            pose_only=[False, True], segment_id=["-50", "-51"],
        )
    assert not sent


def cancellation_client(response, attack_keys=()):
    import threading

    client, sent = capture_client(response)
    client.viz_stream_lock = threading.RLock()
    client._attack_vehicle_keys = set(attack_keys)
    return client, sent


def cancelled_record(vehicle_id, **changes):
    return {"vehicleId": vehicle_id, "status": "ok", "controlMode": "native",
            "externalPoseAuthoritative": False, "cancelled": True, **changes}


def test_cancel_single_exact_wire_and_confirmed_annotation_cleanup():
    response = {"messageType": "cancelCoSimVeh", "status": "ok",
                "data": [cancelled_record("7")]}
    client, sent = cancellation_client(response, [(True, "7"), (False, "7"), (True, "8")])

    actual = client.cancel_cosim_vehicle(7, private_veh=True)

    assert actual is response
    assert sent == [({"messageType": "cancelCoSimVeh", "data": [
        {"vehicleId": 7, "isPrivate": True}]}, True)]
    assert client._attack_vehicle_keys == {(False, "7"), (True, "8")}


@pytest.mark.parametrize("flags,expected", [(True, [True, True]), ((True, False), [True, False])])
def test_cancel_batch_private_flag_broadcast_or_tuple(flags, expected):
    response = {"messageType": "cancelCoSimVeh", "status": "ok", "data": []}
    client, sent = cancellation_client(response)

    assert client.cancel_cosim_vehicle((7, 8), private_veh=flags) is response

    assert sent == [({"messageType": "cancelCoSimVeh", "data": [
        {"vehicleId": 7, "isPrivate": expected[0]},
        {"vehicleId": 8, "isPrivate": expected[1]}]}, True)]


def test_cancel_partial_reordered_reply_never_clears_failed_or_unrequested_ids():
    response = {"messageType": "cancelCoSimVeh", "status": "partial", "data": [
        cancelled_record(8, status="error", cancelled=False, controlMode="cosim",
                         externalPoseAuthoritative=True),
        cancelled_record(9),  # An unrelated response must not change annotations.
        cancelled_record(7),
    ]}
    client, sent = cancellation_client(response, [(True, "7"), (False, "7"),
                                                  (True, "8"), (True, "9")])

    assert client.cancel_cosim_vehicle([7, 8], private_veh=True) is response

    assert len(sent) == 1
    assert client._attack_vehicle_keys == {(False, "7"), (True, "8"), (True, "9")}


@pytest.mark.parametrize("changes", [
    {"status": "error"}, {"cancelled": False}, {"cancelled": 1},
    {"controlMode": "cosim"}, {"externalPoseAuthoritative": True},
    {"externalPoseAuthoritative": None}, {"vehicleId": None}, {"vehicleId": 8},
    {"isPrivate": False}, {"isPrivate": "true"},
])
def test_cancel_incomplete_or_mismatched_success_keeps_annotation(changes):
    response = {"messageType": "cancelCoSimVeh", "status": "ok",
                "data": [cancelled_record(7, **changes)]}
    client, sent = cancellation_client(response, [(True, "7")])

    assert client.cancel_cosim_vehicle(7, private_veh=True) is response

    assert len(sent) == 1
    assert client._attack_vehicle_keys == {(True, "7")}


@pytest.mark.parametrize("record", [None, "invalid", {}, {"vehicleId": 7, "status": "ok"}])
def test_cancel_malformed_record_keeps_annotation_and_reply(record):
    response = {"messageType": "cancelCoSimVeh", "status": "partial", "data": [record]}
    client, _ = cancellation_client(response, [(True, "7")])

    assert client.cancel_cosim_vehicle(7, private_veh=True) is response
    assert client._attack_vehicle_keys == {(True, "7")}


@pytest.mark.parametrize("changes", [
    {"messageType": "reachDest"}, {"status": "error"}, {"status": []}, {"data": None}, {"data": {}},
])
def test_cancel_invalid_envelope_is_returned_for_caller_validation(changes):
    response = {"messageType": "cancelCoSimVeh", "status": "ok",
                "data": [cancelled_record(7)], **changes}
    client, _ = cancellation_client(response, [(True, "7")])

    assert client.cancel_cosim_vehicle(7, private_veh=True) is response
    assert client._attack_vehicle_keys == {(True, "7")}


def test_cancel_ambiguous_public_private_id_requires_response_namespace():
    response = {"messageType": "cancelCoSimVeh", "status": "ok",
                "data": [cancelled_record(7)]}
    client, _ = cancellation_client(response, [(True, "7"), (False, "7")])

    assert client.cancel_cosim_vehicle([7, 7], private_veh=[True, False]) is response
    assert client._attack_vehicle_keys == {(True, "7"), (False, "7")}

    response["data"] = [cancelled_record(7, isPrivate=False)]
    assert client.cancel_cosim_vehicle([7, 7], private_veh=[True, False]) is response
    assert client._attack_vehicle_keys == {(True, "7")}


@pytest.mark.parametrize("flags", [[True], [True, False, True], None, 1, "true", [True, 0]])
def test_cancel_invalid_private_flags_rejected_before_any_send(flags):
    response = {"messageType": "cancelCoSimVeh", "status": "ok", "data": []}
    client, sent = cancellation_client(response, [(True, "7")])

    with pytest.raises(ValueError, match="private_veh"):
        client.cancel_cosim_vehicle([7, 8], private_veh=flags)

    assert sent == []
    assert client._attack_vehicle_keys == {(True, "7")}
