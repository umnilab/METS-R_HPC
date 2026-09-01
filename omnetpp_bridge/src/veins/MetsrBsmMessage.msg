import veins.modules.messages.DemoSafetyMessage;

namespace metsr::veinsbridge;

// Opaque application payload transported through the real Veins PHY/MAC.
packet MetsrBsmMessage extends veins::DemoSafetyMessage {
    string transmissionId;
    string messageId;
    string messageName;
    string messageStandard;
    string expectedReceiverIds;
    string wireDataB64;
    string payloadEncoding;
    string content;
    string attackId;
    string attackType;
    int tick;
    int senderId;
    int intendedReceiverId = -1;
    int messageCount;
    int payloadBytes;
    int payloadBitLength;
    bool attacked;
    double requestTxTimeS;
    double bridgeTxTimeS;
}
