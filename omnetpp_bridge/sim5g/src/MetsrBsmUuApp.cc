#ifdef METSR_WITH_SIMU5G

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <omnetpp.h>

#include <inet/common/TimeTag_m.h>
#include <inet/common/packet/Packet.h>
#include <inet/common/packet/chunk/ByteCountChunk.h>
#include <inet/common/packet/chunk/BytesChunk.h>
#include <inet/networklayer/common/L3Address.h>
#include <inet/networklayer/common/L3AddressResolver.h>
#include <inet/transportlayer/contract/udp/UdpSocket.h>

#include "MetsrVeinsBridgeProtocol.h"

namespace metsr::veinsbridge::sim5g {

using namespace omnetpp;
using namespace inet;

namespace {

std::string parString(cMessage* message, const char* name, const std::string& fallback = "")
{
    return message->hasPar(name) ? message->par(name).stringValue() : fallback;
}

int parInt(cMessage* message, const char* name, int fallback = 0)
{
    return message->hasPar(name) ? static_cast<int>(message->par(name).longValue()) : fallback;
}

int base64Value(char value)
{
    if (value >= 'A' && value <= 'Z') return value - 'A';
    if (value >= 'a' && value <= 'z') return value - 'a' + 26;
    if (value >= '0' && value <= '9') return value - '0' + 52;
    if (value == '+') return 62;
    if (value == '/') return 63;
    return -1;
}

std::vector<uint8_t> decodeBase64(const std::string& text)
{
    std::string encoded;
    for (char value : text) {
        if (!std::isspace(static_cast<unsigned char>(value))) encoded.push_back(value);
    }
    if (encoded.size() % 4 != 0) {
        throw std::invalid_argument("Uu payload base64 length must be a multiple of four");
    }
    std::vector<uint8_t> bytes;
    for (size_t offset = 0; offset < encoded.size(); offset += 4) {
        const bool lastBlock = offset + 4 == encoded.size();
        const char c0 = encoded[offset];
        const char c1 = encoded[offset + 1];
        const char c2 = encoded[offset + 2];
        const char c3 = encoded[offset + 3];
        const int v0 = base64Value(c0);
        const int v1 = base64Value(c1);
        const int v2 = c2 == '=' ? 0 : base64Value(c2);
        const int v3 = c3 == '=' ? 0 : base64Value(c3);
        if (v0 < 0 || v1 < 0 || v2 < 0 || v3 < 0 ||
            (!lastBlock && (c2 == '=' || c3 == '=')) ||
            (c2 == '=' && c3 != '=')) {
            throw std::invalid_argument("Uu payload contains invalid base64");
        }
        const uint32_t value = (static_cast<uint32_t>(v0) << 18) |
            (static_cast<uint32_t>(v1) << 12) |
            (static_cast<uint32_t>(v2) << 6) |
            static_cast<uint32_t>(v3);
        bytes.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
        if (c2 != '=') bytes.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
        if (c3 != '=') bytes.push_back(static_cast<uint8_t>(value & 0xff));
    }
    return bytes;
}

std::string encodeBase64(const std::vector<uint8_t>& bytes)
{
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string encoded;
    for (size_t offset = 0; offset < bytes.size(); offset += 3) {
        const size_t remaining = bytes.size() - offset;
        const uint32_t value = (static_cast<uint32_t>(bytes[offset]) << 16) |
            (remaining > 1 ? static_cast<uint32_t>(bytes[offset + 1]) << 8 : 0) |
            (remaining > 2 ? static_cast<uint32_t>(bytes[offset + 2]) : 0);
        encoded.push_back(alphabet[(value >> 18) & 0x3f]);
        encoded.push_back(alphabet[(value >> 12) & 0x3f]);
        encoded.push_back(remaining > 1 ? alphabet[(value >> 6) & 0x3f] : '=');
        encoded.push_back(remaining > 2 ? alphabet[value & 0x3f] : '=');
    }
    return encoded;
}

std::string packetPayloadMode(Packet* packet)
{
    const std::string name = packet->getName();
    const std::string prefix = "MetsrBsm|";
    if (name.rfind(prefix, 0) != 0) return "unknown";
    const std::string remainder = name.substr(prefix.size());
    const size_t separator = remainder.find('|');
    return separator == std::string::npos ? "unknown" : remainder.substr(0, separator);
}

std::string packetMessageId(Packet* packet)
{
    const std::string name = packet->getName();
    const std::string prefix = "MetsrBsm|";
    if (name.rfind(prefix, 0) == 0) {
        const std::string remainder = name.substr(prefix.size());
        const size_t separator = remainder.find('|');
        return separator == std::string::npos ? remainder : remainder.substr(separator + 1);
    }
    return name;
}

} // namespace

class MetsrBsmUuApp : public cSimpleModule, public UdpSocket::ICallback {
  private:
    UdpSocket socket;
    int localPort = 4400;
    int defaultDestPort = 4400;
    std::string bridgeModulePath;
    cModule* bridgeModule = nullptr;
    long sentCount = 0;
    long receivedCount = 0;

  protected:
    int numInitStages() const override { return inet::NUM_INIT_STAGES; }
    void initialize(int stage) override;
    void handleMessage(cMessage* message) override;
    void refreshDisplay() const override;

    void sendBsm(cMessage* request);
    void reportReceived(Packet* packet);
    cModule* resolveBridgeModule();

    void socketDataArrived(UdpSocket* socket, Packet* packet) override;
    void socketErrorArrived(UdpSocket* socket, Indication* indication) override;
    void socketClosed(UdpSocket* socket) override;
};

Define_Module(MetsrBsmUuApp);

void MetsrBsmUuApp::initialize(int stage)
{
    cSimpleModule::initialize(stage);
    if (stage != inet::INITSTAGE_APPLICATION_LAYER) {
        return;
    }

    localPort = par("localPort");
    defaultDestPort = par("destPort");
    bridgeModulePath = par("bridgeModule").stringValue();

    socket.setOutputGate(gate("socketOut"));
    socket.bind(localPort);
    socket.setCallback(this);

    int tos = par("tos");
    if (tos != -1) {
        socket.setTos(tos);
    }
}

void MetsrBsmUuApp::handleMessage(cMessage* message)
{
    if (message->getKind() == metsr::veinsbridge::KIND_SIMU5G_BSM_REQUEST) {
        std::unique_ptr<cMessage> cleanup(message);
        sendBsm(message);
        return;
    }

    socket.processMessage(message);
}

void MetsrBsmUuApp::sendBsm(cMessage* request)
{
    const std::string messageId = parString(request, "message_id");
    const std::string destAddressText = parString(request, "dest_address");
    const int destPort = parInt(request, "dest_port", defaultDestPort);
    const int declaredPayloadBytes = std::max(0, parInt(request, "payload_bytes", 0));
    const bool hasWirePayload = request->hasPar("wire_payload_b64");
    const std::string wirePayloadB64 = parString(request, "wire_payload_b64");

    if (messageId.empty() || destAddressText.empty()) {
        EV_WARN << "MetsrBsmUuApp dropping malformed bridge request\n";
        return;
    }

    auto destAddress = L3AddressResolver().resolve(destAddressText.c_str());
    auto* packet = new Packet(
        ("MetsrBsm|" + std::string(hasWirePayload ? "wire|" : "size|") +
         messageId).c_str());
    try {
        if (hasWirePayload) {
            const std::vector<uint8_t> wirePayload = decodeBase64(wirePayloadB64);
            if (declaredPayloadBytes > 0 &&
                declaredPayloadBytes != static_cast<int>(wirePayload.size())) {
                throw cRuntimeError(
                    "Uu request payload_bytes=%d does not match decoded payload length=%d",
                    declaredPayloadBytes,
                    static_cast<int>(wirePayload.size()));
            }
            auto payload = makeShared<BytesChunk>(wirePayload);
            payload->addTag<CreationTimeTag>()->setCreationTime(simTime());
            packet->insertAtBack(payload);
        }
        else {
            auto payload = makeShared<ByteCountChunk>(
                B(std::max(1, declaredPayloadBytes)));
            payload->addTag<CreationTimeTag>()->setCreationTime(simTime());
            packet->insertAtBack(payload);
        }
    }
    catch (...) {
        delete packet;
        throw;
    }

    socket.sendTo(packet, destAddress, destPort);
    sentCount += 1;
}

void MetsrBsmUuApp::socketDataArrived(UdpSocket*, Packet* packet)
{
    std::unique_ptr<Packet> cleanup(packet);
    reportReceived(packet);
    receivedCount += 1;
}

void MetsrBsmUuApp::reportReceived(Packet* packet)
{
    cModule* bridge = resolveBridgeModule();
    if (bridge == nullptr || !bridge->hasGate(metsr::veinsbridge::SIMU5G_REPORT_GATE)) {
        EV_WARN << "MetsrBsmUuApp cannot report receive event; bridge module/gate not found\n";
        return;
    }

    auto* report = new cMessage("simu5gBsmRxReport", metsr::veinsbridge::KIND_SIMU5G_RX_REPORT);
    report->addPar("message_id") = packetMessageId(packet).c_str();
    report->addPar("packet_name") = packet->getName();
    report->addPar("receiver_module") = getParentModule()->getFullPath().c_str();
    report->addPar("receiver_app") = getFullPath().c_str();
    report->addPar("receiver_index") = getParentModule()->getIndex();
    report->addPar("sender_module") = "";
    report->addPar("payload_bytes") = static_cast<long>(packet->getByteLength());
    const bool hasWirePayload = packetPayloadMode(packet) == "wire";
    report->addPar("wire_payload_present") = hasWirePayload;
    report->addPar("wire_payload_encoding") = hasWirePayload ? "base64" : "";
    if (hasWirePayload) {
        const std::string payloadBase64 = encodeBase64(
            packet->peekDataAsBytes()->getBytes());
        report->addPar("wire_payload_b64") = payloadBase64.c_str();
    }
    else {
        report->addPar("wire_payload_b64") = "";
    }
    sendDirect(report, bridge, metsr::veinsbridge::SIMU5G_REPORT_GATE);
}

cModule* MetsrBsmUuApp::resolveBridgeModule()
{
    if (bridgeModule != nullptr) {
        return bridgeModule;
    }
    bridgeModule = getModuleByPath(bridgeModulePath.c_str());
    if (bridgeModule == nullptr && getSimulation() != nullptr) {
        bridgeModule = getSimulation()->getModuleByPath(bridgeModulePath.c_str());
    }
    return bridgeModule;
}

void MetsrBsmUuApp::socketErrorArrived(UdpSocket*, Indication* indication)
{
    EV_WARN << "Ignoring UDP error report " << indication->getName() << "\n";
    delete indication;
}

void MetsrBsmUuApp::socketClosed(UdpSocket*)
{
}

void MetsrBsmUuApp::refreshDisplay() const
{
    char buffer[96];
    snprintf(buffer, sizeof(buffer), "BSM sent: %ld\nBSM rx: %ld", sentCount, receivedCount);
    getDisplayString().setTagArg("t", 0, buffer);
}

} // namespace metsr::veinsbridge::sim5g

#endif // METSR_WITH_SIMU5G
