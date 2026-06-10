simple MetsrVeinsBridge
{
    parameters:
        string bridgeHost = default("0.0.0.0");
        int bridgePort = default(9099);
        double baseLatencyMs = default(8);
        double perMessageLatencyMs = default(0.35);
        double perPayloadByteLatencyUs = default(0.04);
        double contentionLossSlope = default(0.002);
        string radioAccess = default("cv2x");
        @display("i=block/network2");
}
