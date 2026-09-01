#ifdef METSR_WITH_VEINS

#include "MetsrVeinsApp.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "MetsrBsmMessage_m.h"
#include "MetsrVeinsBridgeProtocol.h"
#include "veins/base/phyLayer/PhyToMacControlInfo.h"
#include "veins/modules/phy/DeciderResult80211.h"

namespace metsr::veinsbridge {

using namespace omnetpp;

namespace {

std::string stringPar(cMessage* message, const char* name, const std::string& fallback = "")
{
    return message->hasPar(name) ? message->par(name).stringValue() : fallback;
}

long longPar(cMessage* message, const char* name, long fallback = 0)
{
    return message->hasPar(name) ? message->par(name).longValue() : fallback;
}

double doublePar(cMessage* message, const char* name, double fallback = 0.0)
{
    return message->hasPar(name) ? message->par(name).doubleValue() : fallback;
}

bool receiverIsExpected(const std::string& csv, int receiverId)
{
    if (csv.empty()) {
        return true;
    }
    std::istringstream input(csv);
    std::string token;
    while (std::getline(input, token, ',')) {
        try {
            if (std::stoi(token) == receiverId) {
                return true;
            }
        }
        catch (...) {
        }
    }
    return false;
}

int base64DecodedBytes(const std::string& encoded)
{
    if (encoded.empty()) {
        return -1;
    }
    if (encoded.size() % 4 != 0) {
        throw cRuntimeError("METS-R wire_data_b64 length is not a multiple of four");
    }
    std::size_t padding = 0;
    if (!encoded.empty() && encoded.back() == '=') {
        padding += 1;
    }
    if (encoded.size() > 1 && encoded[encoded.size() - 2] == '=') {
        padding += 1;
    }
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        const unsigned char value = static_cast<unsigned char>(encoded[i]);
        const bool alphabet = std::isalnum(value) || value == '+' || value == '/';
        const bool validPadding = value == '=' && i >= encoded.size() - padding;
        if (!alphabet && !validPadding) {
            throw cRuntimeError("METS-R wire_data_b64 contains an invalid character");
        }
    }
    return static_cast<int>((encoded.size() / 4) * 3 - padding);
}

double injectionDelayS(const std::string& transmissionId, double spreadS)
{
    if (spreadS <= 0.0) {
        return 0.0;
    }
    std::uint32_t hash = 2166136261u;
    for (const unsigned char value : transmissionId) {
        hash ^= value;
        hash *= 16777619u;
    }
    constexpr double denominator = 4294967296.0;
    return spreadS * (static_cast<double>(hash) + 0.5) / denominator;
}

} // namespace

Define_Module(MetsrVeinsApp);

void MetsrVeinsApp::initialize(int stage)
{
    veins::DemoBaseApplLayer::initialize(stage);
    if (stage == 0) {
        externalVehicleId = par("externalVehicleId").intValue();
    }
}

void MetsrVeinsApp::handleMessage(cMessage* message)
{
    if (!message->isSelfMessage() && message->getArrivalGate() == gate("bridgeIn")) {
        if (message->getKind() != KIND_VEINS_BSM_REQUEST) {
            delete message;
            throw cRuntimeError("MetsrVeinsApp received an unknown bridge request");
        }
        injectBsm(message);
        return;
    }
    veins::DemoBaseApplLayer::handleMessage(message);
}

void MetsrVeinsApp::injectBsm(cMessage* request)
{
    std::unique_ptr<cMessage> cleanup(request);
    auto* bsm = new MetsrBsmMessage("METS-R J2735 BSM");
    const auto recipientMac = static_cast<veins::LAddress::L2Type>(
        longPar(request, "recipient_mac", veins::LAddress::L2BROADCAST()));
    populateWSM(bsm, recipientMac);

    const std::string transmissionId = stringPar(request, "transmission_id");
    bsm->setTransmissionId(transmissionId.c_str());
    bsm->setMessageId(stringPar(request, "message_id").c_str());
    bsm->setMessageName(stringPar(request, "message_name").c_str());
    bsm->setMessageStandard(stringPar(request, "message_standard").c_str());
    bsm->setExpectedReceiverIds(stringPar(request, "expected_receiver_ids").c_str());
    const std::string wireDataB64 = stringPar(request, "wire_data_b64");
    bsm->setWireDataB64(wireDataB64.c_str());
    bsm->setPayloadEncoding(stringPar(request, "payload_encoding").c_str());
    bsm->setContent(stringPar(request, "content").c_str());
    bsm->setAttackId(stringPar(request, "attack_id").c_str());
    bsm->setAttackType(stringPar(request, "attack_type").c_str());
    bsm->setTick(static_cast<int>(longPar(request, "tick")));
    bsm->setSenderId(static_cast<int>(longPar(request, "sender_id")));
    bsm->setIntendedReceiverId(static_cast<int>(longPar(request, "receiver_id", -1)));
    bsm->setMessageCount(static_cast<int>(longPar(request, "message_count")));
    const int encodedBytes = base64DecodedBytes(wireDataB64);
    const int payloadBytes = encodedBytes >= 0
        ? encodedBytes
        : std::max(0L, longPar(request, "payload_bytes"));
    const int payloadBitLength = encodedBytes >= 0
        ? encodedBytes * 8
        : std::max(0L, longPar(request, "payload_bit_length", payloadBytes * 8));
    bsm->setPayloadBytes(payloadBytes);
    bsm->setPayloadBitLength(payloadBitLength);
    bsm->setAttacked(longPar(request, "attacked") != 0);
    bsm->setRequestTxTimeS(doublePar(request, "tx_time_s", simTime().dbl()));
    const double delayS = injectionDelayS(
        transmissionId,
        par("bridgeInjectionSpread").doubleValue());
    bsm->setBridgeTxTimeS(simTime().dbl() + delayS);

    bsm->setBitLength(static_cast<int64_t>(headerLength) + payloadBitLength);
    if (delayS > 0.0) {
        sendDelayedDown(bsm, delayS);
    }
    else {
        sendDown(bsm);
    }
}

void MetsrVeinsApp::onBSM(veins::DemoSafetyMessage* frame)
{
    auto* bsm = dynamic_cast<MetsrBsmMessage*>(frame);
    if (bsm == nullptr) {
        return;
    }
    if (bsm->getIntendedReceiverId() > 0 &&
        bsm->getIntendedReceiverId() != externalVehicleId) {
        return;
    }
    if (!receiverIsExpected(bsm->getExpectedReceiverIds(), externalVehicleId)) {
        return;
    }

    auto* report = new cMessage("metsrVeinsRxReport", KIND_VEINS_RX_REPORT);
    report->addPar("transmission_id") = bsm->getTransmissionId();
    report->addPar("message_id") = bsm->getMessageId();
    report->addPar("message_name") = bsm->getMessageName();
    report->addPar("message_standard") = bsm->getMessageStandard();
    report->addPar("payload_encoding") = bsm->getPayloadEncoding();
    report->addPar("wire_data_b64") = bsm->getWireDataB64();
    report->addPar("sender_id") = bsm->getSenderId();
    report->addPar("receiver_id") = externalVehicleId;
    report->addPar("message_count") = bsm->getMessageCount();
    report->addPar("payload_bytes") = bsm->getPayloadBytes();
    report->addPar("payload_bit_length") = bsm->getPayloadBitLength();
    report->addPar("bridge_tx_time_s") = bsm->getBridgeTxTimeS();
    report->addPar("receiver_mac") = static_cast<long>(myId);
    report->addPar("receiver_module") = getParentModule()->getFullPath().c_str();

    auto* control = dynamic_cast<veins::PhyToMacControlInfo*>(bsm->getControlInfo());
    auto* result = control != nullptr
        ? dynamic_cast<veins::DeciderResult80211*>(control->getDeciderResult())
        : nullptr;
    if (control != nullptr) {
        report->addPar("sender_mac") = static_cast<long>(control->getSourceAddress());
    }
    if (result != nullptr) {
        const double snirLinear = result->getSnr();
        report->addPar("snir_linear") = snirLinear;
        report->addPar("snir_db") =
            snirLinear > 0.0 ? 10.0 * std::log10(snirLinear) : -INFINITY;
        report->addPar("recv_power_dbm") = result->getRecvPower_dBm();
        report->addPar("bitrate_bps") = result->getBitrate();
    }

    cModule* bridgeModule = resolveBridge();
    if (bridgeModule == nullptr || !bridgeModule->hasGate(VEINS_REPORT_GATE)) {
        delete report;
        throw cRuntimeError("Could not resolve the METS-R bridge report gate");
    }
    sendDirect(report, bridgeModule, VEINS_REPORT_GATE);
}

cModule* MetsrVeinsApp::resolveBridge()
{
    if (bridge != nullptr) {
        return bridge;
    }
    const char* configuredPath = par("bridgeModulePath").stringValue();
    if (configuredPath != nullptr && configuredPath[0] != '\0') {
        bridge = getModuleByPath(configuredPath);
    }
    if (bridge == nullptr && getParentModule() != nullptr &&
        getParentModule()->getParentModule() != nullptr) {
        bridge = getParentModule()->getParentModule()->getSubmodule("bridge");
    }
    return bridge;
}

} // namespace metsr::veinsbridge

#endif // METSR_WITH_VEINS
