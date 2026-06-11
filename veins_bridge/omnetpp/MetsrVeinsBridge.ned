simple MetsrVeinsBridge
{
    parameters:
        string bridgeHost = default("0.0.0.0");
        int bridgePort = default(9099);
        double baseLatencyMs = default(8);
        double perMessageLatencyMs = default(0.35);
        double perPayloadByteLatencyUs = default(0.04);
        double contentionLossSlope = default(0.0002);
        double bitrateMbps = default(6);
        double macSlotTimeMs = default(0.013);
        double maxJitterMs = default(0.25);
        double communicationRangeM = default(1000);
        double distanceLatencyUsPerM = default(2);
        double distanceLossAtRange = default(0.05);
        int pollIntervalMs = default(10);
        string bridgeBackend = default("abstract_omnetpp");
        string backendImplementation = default("abstract_event_profile");
        string networkModel = default("omnetpp_event_wireless_queue_model");
        string backendNote = default("");
        string radioAccess = default("cv2x");
        bool logRequests = default(true);
        @display("i=block/network2");
}
