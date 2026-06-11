#ifdef METSR_WITH_SIMU5G

#include <algorithm>
#include <memory>
#include <string>

#include <omnetpp.h>

#include <inet/common/TimeTag_m.h>
#include <inet/common/packet/Packet.h>
#include <inet/common/packet/chunk/ByteCountChunk.h>
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

std::string packetMessageId(Packet* packet)
{
    const std::string name = packet->getName();
    const std::string prefix = "MetsrBsm|";
    if (name.rfind(prefix, 0) == 0) {
        return name.substr(prefix.size());
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
    const int payloadBytes = std::max(1, parInt(request, "payload_bytes", 1));

    if (messageId.empty() || destAddressText.empty()) {
        EV_WARN << "MetsrBsmUuApp dropping malformed bridge request\n";
        return;
    }

    auto destAddress = L3AddressResolver().resolve(destAddressText.c_str());
    auto* packet = new Packet(("MetsrBsm|" + messageId).c_str());
    auto payload = makeShared<ByteCountChunk>(B(payloadBytes));
    payload->addTag<CreationTimeTag>()->setCreationTime(simTime());
    packet->insertAtBack(payload);

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
    report->addPar("sender_module") = "";
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
