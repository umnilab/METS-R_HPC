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
        int pollIntervalMs = default(10);
        string radioAccess = default("cv2x");
        bool logRequests = default(true);
        @display("i=block/network2");
}
