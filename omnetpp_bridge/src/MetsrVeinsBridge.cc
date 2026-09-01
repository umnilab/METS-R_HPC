#include <omnetpp.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <cmath>
#include <cstring>
#include <deque>
#include <iostream>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <nlohmann/json.hpp>

#include "MetsrVeinsBridgeProtocol.h"

#ifdef METSR_WITH_VEINS
#include "veins/base/connectionManager/BaseConnectionManager.h"
#endif

using json = nlohmann::json;

namespace {

constexpr const char* PROTOCOL_NAME = "metsr-veins-jsonl";
constexpr int PROTOCOL_VERSION = 1;
constexpr const char* DEFAULT_NETWORK_MODEL = "omnetpp_event_wireless_queue_model";
constexpr double SPEED_OF_LIGHT_MPS = 299792458.0;

using metsr::veinsbridge::KIND_KEEP_ALIVE;
using metsr::veinsbridge::KIND_PACKET_DELIVERY;
using metsr::veinsbridge::KIND_SIMU5G_BSM_REQUEST;
using metsr::veinsbridge::KIND_SIMU5G_MOBILITY_UPDATE;
using metsr::veinsbridge::KIND_SIMU5G_RX_REPORT;
using metsr::veinsbridge::KIND_SIMU5G_SYNC_TIMEOUT;
using metsr::veinsbridge::KIND_SYNC_TICK_BOUNDARY;
using metsr::veinsbridge::KIND_VEINS_BSM_REQUEST;
using metsr::veinsbridge::KIND_VEINS_MOBILITY_UPDATE;
using metsr::veinsbridge::KIND_VEINS_RX_REPORT;
using metsr::veinsbridge::KIND_VEINS_SYNC_TIMEOUT;
using metsr::veinsbridge::SIMU5G_BRIDGE_GATE;
using metsr::veinsbridge::SIMU5G_REPORT_GATE;
using metsr::veinsbridge::VEINS_BRIDGE_GATE;
using metsr::veinsbridge::VEINS_REPORT_GATE;

struct SyncWork {
    int requestId = 0;
    int tick = 0;
    json request;
    json received = json::array();
    json metrics = json::array();
    json attackEvents = json::array();
    int outstandingDeliveries = 0;
    omnetpp::cMessage* timeoutEvent = nullptr;
    omnetpp::cMessage* boundaryEvent = nullptr;
    double durationS = 0.0;
    omnetpp::simtime_t tickStartTime;
    omnetpp::simtime_t tickEndTime;
    bool completionPending = false;
    bool done = false;
    json response;
    std::mutex mutex;
    std::condition_variable cv;
};

struct PacketDelivery {
    std::shared_ptr<SyncWork> work;
    json message;
    json metric;
    int tick = 0;
    int senderId = 0;
    int receiverId = 0;
    double generationTimeS = 0.0;
};

struct Simu5gPendingDelivery {
    std::shared_ptr<SyncWork> work;
    json message;
    json metric;
    double generationTimeS = 0.0;
};

// PC5 multicast deliveries are tracked per physical transmission.
struct Simu5gPc5PendingTransmission {
    std::shared_ptr<SyncWork> work;
    json message;
    std::map<int, json> messageByReceiverIndex;
    std::map<int, int> receiverIdByIndex;
    json metric;
    std::set<int> expectedReceiverIndexes;
    std::set<int> completedReceiverIndexes;
    double generationTimeS = 0.0;
    std::string transmissionId;
    std::string wirePayloadB64;
};

struct VeinsPendingTransmission {
    std::shared_ptr<SyncWork> work;
    json message;
    std::map<int, json> messageByReceiver;
    json metric;
    std::set<int> expectedReceivers;
    std::set<int> receivedReceivers;
    double generationTimeS = 0.0;
    std::string transmissionId;
};

struct SyncWorkHolder {
    std::shared_ptr<SyncWork> work;
};

double numberValue(const json& record, const char* key, double fallback = 0.0)
{
    auto it = record.find(key);
    if (it == record.end() || it->is_null()) {
        return fallback;
    }
    if (it->is_number()) {
        return it->get<double>();
    }
    if (it->is_string()) {
        try {
            return std::stod(it->get<std::string>());
        }
        catch (...) {
            return fallback;
        }
    }
    return fallback;
}

int intValue(const json& record, const char* key, int fallback = 0)
{
    auto it = record.find(key);
    if (it == record.end() || it->is_null()) {
        return fallback;
    }
    if (it->is_number_integer()) {
        return it->get<int>();
    }
    if (it->is_number()) {
        return static_cast<int>(it->get<double>());
    }
    if (it->is_string()) {
        try {
            return std::stoi(it->get<std::string>());
        }
        catch (...) {
            return fallback;
        }
    }
    return fallback;
}

bool hasKey(const json& record, const char* key)
{
    return record.find(key) != record.end() && !record.at(key).is_null();
}

std::string stringValue(const json& record, const char* key, const std::string& fallback = "")
{
    auto it = record.find(key);
    if (it == record.end() || it->is_null()) {
        return fallback;
    }
    if (it->is_string()) {
        return it->get<std::string>();
    }
    return it->dump();
}

const json* nestedValue(const json& record, std::initializer_list<const char*> path)
{
    const json* current = &record;
    for (const char* key : path) {
        if (current == nullptr || !current->is_object()) {
            return nullptr;
        }
        auto it = current->find(key);
        if (it == current->end() || it->is_null()) {
            return nullptr;
        }
        current = &(*it);
    }
    return current;
}

std::string stringFromJsonValue(const json& value, const std::string& fallback = "")
{
    if (value.is_null()) {
        return fallback;
    }
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return value.dump();
}

int intFromJsonValue(const json& value, int fallback = 0)
{
    if (value.is_null()) {
        return fallback;
    }
    if (value.is_number_integer()) {
        return value.get<int>();
    }
    if (value.is_number()) {
        return static_cast<int>(value.get<double>());
    }
    if (value.is_string()) {
        try {
            return std::stoi(value.get<std::string>());
        }
        catch (...) {
            return fallback;
        }
    }
    return fallback;
}

double numberFromJsonValue(const json& value, double fallback = 0.0)
{
    if (value.is_null()) {
        return fallback;
    }
    if (value.is_number()) {
        return value.get<double>();
    }
    if (value.is_string()) {
        try {
            return std::stod(value.get<std::string>());
        }
        catch (...) {
            return fallback;
        }
    }
    return fallback;
}

std::string nestedStringValue(
    const json& record,
    std::initializer_list<const char*> path,
    const std::string& fallback = "")
{
    const json* value = nestedValue(record, path);
    return value == nullptr ? fallback : stringFromJsonValue(*value, fallback);
}

int nestedIntValue(const json& record, std::initializer_list<const char*> path, int fallback = 0)
{
    const json* value = nestedValue(record, path);
    return value == nullptr ? fallback : intFromJsonValue(*value, fallback);
}

double nestedNumberValue(
    const json& record,
    std::initializer_list<const char*> path,
    double fallback = 0.0)
{
    const json* value = nestedValue(record, path);
    return value == nullptr ? fallback : numberFromJsonValue(*value, fallback);
}

json nestedJsonValue(const json& record, std::initializer_list<const char*> path, const json& fallback)
{
    const json* value = nestedValue(record, path);
    return value == nullptr ? fallback : *value;
}

int entityId(const json& record, const char* primary, int fallback);

int bsmSenderId(const json& message)
{
    const int flatSenderId = entityId(message, "sender_id", entityId(message, "vehicle_id", 0));
    if (flatSenderId != 0) {
        return flatSenderId;
    }
    return nestedIntValue(message, {"transport", "senderId"}, 0);
}

int bsmReceiverId(const json& message)
{
    if (hasKey(message, "receiver_id")) {
        return intValue(message, "receiver_id", 0);
    }
    if (hasKey(message, "target_vehicle_id")) {
        return intValue(message, "target_vehicle_id", 0);
    }
    return nestedIntValue(message, {"transport", "receiverId"}, 0);
}

int bsmPayloadBytes(const json& message)
{
    if (hasKey(message, "payload_bytes")) {
        return intValue(message, "payload_bytes", 0);
    }
    const int wireBytes =
        nestedIntValue(message, {"wire_payload", "byte_length"}, -1);
    if (wireBytes >= 0) {
        return wireBytes;
    }
    return nestedIntValue(message, {"transport", "payloadBytes"}, 0);
}

double bsmTxTimeS(const json& message, double fallback)
{
    if (hasKey(message, "tx_time_s")) {
        return numberValue(message, "tx_time_s", fallback);
    }
    return nestedNumberValue(message, {"transport", "txTimeS"}, fallback);
}

int bsmMessageCount(const json& message)
{
    if (hasKey(message, "message_count")) {
        return intValue(message, "message_count", 0);
    }
    int count = nestedIntValue(message, {"messageFrame", "value", "BasicSafetyMessage", "coreData", "msgCnt"}, -1);
    if (count >= 0) {
        return count;
    }
    count = nestedIntValue(message, {"BasicSafetyMessage", "coreData", "msgCnt"}, -1);
    if (count >= 0) {
        return count;
    }
    return nestedIntValue(message, {"coreData", "msgCnt"}, 0);
}

std::string bsmMessageName(const json& message)
{
    const std::string flatName = stringValue(message, "message_name");
    if (!flatName.empty()) {
        return flatName;
    }
    const std::string frameName = nestedStringValue(message, {"messageFrame", "messageId"});
    if (!frameName.empty()) {
        if (frameName == "basicSafetyMessage") {
            return "BasicSafetyMessage";
        }
        return frameName;
    }
    if (nestedValue(message, {"messageFrame", "value", "BasicSafetyMessage"}) != nullptr ||
        nestedValue(message, {"BasicSafetyMessage"}) != nullptr) {
        return "BasicSafetyMessage";
    }
    return "";
}

std::string bsmMessageStandard(const json& message)
{
    const std::string flatStandard = stringValue(message, "message_standard");
    if (!flatStandard.empty()) {
        return flatStandard;
    }
    return nestedStringValue(message, {"transport", "messageStandard"}, "SAE J2735");
}

std::string bsmRadioMode(const json& message, const std::string& fallback)
{
    const std::string flatMode = stringValue(message, "radio_mode");
    if (!flatMode.empty()) {
        return flatMode;
    }
    return nestedStringValue(message, {"transport", "radioMode"}, fallback);
}

std::string bsmContent(const json& message)
{
    const std::string flatContent = stringValue(message, "content");
    if (!flatContent.empty()) {
        return flatContent;
    }
    return nestedStringValue(message, {"operationalData", "content"});
}

json bsmAttackValue(const json& message, const char* flatKey, const char* nestedKey, const json& fallback)
{
    if (hasKey(message, flatKey)) {
        return message.at(flatKey);
    }
    return nestedJsonValue(message, {"operationalData", "attack", nestedKey}, fallback);
}

std::string bsmAttackString(const json& message, const char* flatKey, const char* nestedKey)
{
    const std::string flatValue = stringValue(message, flatKey);
    if (!flatValue.empty()) {
        return flatValue;
    }
    return nestedStringValue(message, {"operationalData", "attack", nestedKey});
}

json bsmPayloadJson(const json& message)
{
    const json* frame = nestedValue(message, {"messageFrame"});
    if (frame != nullptr && frame->is_object()) {
        return *frame;
    }
    const json* bsm = nestedValue(message, {"BasicSafetyMessage"});
    if (bsm != nullptr && bsm->is_object()) {
        return json {
            {"messageId", "BasicSafetyMessage"},
            {"value", json {{"BasicSafetyMessage", *bsm}}},
        };
    }
    return message;
}

std::string bsmWireDataB64(const json& message)
{
    const std::string flat = stringValue(message, "data_b64", stringValue(message, "wire_data_b64"));
    if (!flat.empty()) {
        return flat;
    }
    return nestedStringValue(message, {"wire_payload", "data_b64"});
}

std::string bsmPayloadEncoding(const json& message)
{
    const std::string flat = stringValue(message, "payload_encoding", stringValue(message, "encoding"));
    if (!flat.empty()) {
        return flat;
    }
    return nestedStringValue(message, {"wire_payload", "encoding"}, "json");
}

int bsmPayloadBitLength(const json& message)
{
    if (hasKey(message, "payload_bit_length")) {
        return intValue(message, "payload_bit_length");
    }
    const int nestedBits = nestedIntValue(message, {"wire_payload", "bit_length"}, -1);
    return nestedBits >= 0 ? nestedBits : bsmPayloadBytes(message) * 8;
}

int base64AlphabetValue(char value)
{
    if (value >= 'A' && value <= 'Z') {
        return value - 'A';
    }
    if (value >= 'a' && value <= 'z') {
        return value - 'a' + 26;
    }
    if (value >= '0' && value <= '9') {
        return value - '0' + 52;
    }
    if (value == '+') {
        return 62;
    }
    if (value == '/') {
        return 63;
    }
    return -1;
}

int decodedBase64Length(const std::string& text)
{
    std::string encoded;
    encoded.reserve(text.size());
    for (const char value : text) {
        // Pass the validated bytes through unchanged. The Veins decoder does
        // not accept whitespace and Simu5G re-encodes canonically, so accepting
        // whitespace here would either throw later or create a false mismatch.
        if (std::isspace(static_cast<unsigned char>(value))) {
            return -1;
        }
        encoded.push_back(value);
    }
    if (encoded.empty()) {
        return 0;
    }
    if (encoded.size() % 4 != 0) {
        return -1;
    }

    int decodedBytes = 0;
    for (std::size_t offset = 0; offset < encoded.size(); offset += 4) {
        const bool lastBlock = offset + 4 == encoded.size();
        const char c0 = encoded[offset];
        const char c1 = encoded[offset + 1];
        const char c2 = encoded[offset + 2];
        const char c3 = encoded[offset + 3];
        if (base64AlphabetValue(c0) < 0 ||
            base64AlphabetValue(c1) < 0 ||
            (c2 != '=' && base64AlphabetValue(c2) < 0) ||
            (c3 != '=' && base64AlphabetValue(c3) < 0) ||
            (!lastBlock && (c2 == '=' || c3 == '=')) ||
            (c2 == '=' && c3 != '=')) {
            return -1;
        }
        decodedBytes += 1;
        if (c2 != '=') {
            decodedBytes += 1;
        }
        if (c3 != '=') {
            decodedBytes += 1;
        }
    }
    return decodedBytes;
}

std::string bsmWirePayloadValidationError(const json& message)
{
    const std::string wirePayloadB64 = bsmWireDataB64(message);
    if (wirePayloadB64.empty()) {
        return bsmPayloadEncoding(message) == "uper"
            ? "missing_uper_wire_payload"
            : "";
    }
    const int decodedBytes = decodedBase64Length(wirePayloadB64);
    if (decodedBytes < 0) {
        return "invalid_wire_payload_base64";
    }
    if (bsmPayloadEncoding(message) == "uper" && decodedBytes == 0) {
        return "missing_uper_wire_payload";
    }
    const int declaredBytes = bsmPayloadBytes(message);
    if (declaredBytes > 0 && declaredBytes != decodedBytes) {
        return "wire_payload_byte_length_mismatch";
    }
    const int declaredBits = bsmPayloadBitLength(message);
    if (declaredBits > 0 && declaredBits != decodedBytes * 8) {
        return "wire_payload_bit_length_mismatch";
    }
    return "";
}

std::vector<int> explicitExpectedReceivers(const json& message)
{
    std::vector<int> receivers;
    auto it = message.find("expected_receiver_ids");
    if (it == message.end() || !it->is_array()) {
        return receivers;
    }
    for (const auto& value : *it) {
        const int receiverId = intFromJsonValue(value, 0);
        if (receiverId > 0) {
            receivers.push_back(receiverId);
        }
    }
    return receivers;
}

std::string joinReceiverIds(const std::set<int>& receivers)
{
    std::ostringstream output;
    bool first = true;
    for (const int receiverId : receivers) {
        if (!first) {
            output << ",";
        }
        first = false;
        output << receiverId;
    }
    return output.str();
}

std::string linkMessageId(const std::string& transmissionId, int receiverId)
{
    return transmissionId + ":rx:" + std::to_string(receiverId);
}

std::string messageIdValue(const json& record, int tick, int senderId, int receiverId)
{
    const std::string explicitId = stringValue(record, "message_id");
    if (!explicitId.empty()) {
        return explicitId;
    }
    const std::string transportId = nestedStringValue(record, {"transport", "messageId"});
    if (!transportId.empty()) {
        return transportId;
    }
    const int messageCount = bsmMessageCount(record);
    return std::to_string(tick) + ":" + std::to_string(senderId) + ">" +
        std::to_string(receiverId) + ":" + std::to_string(messageCount);
}

int entityId(const json& record, const char* primary, int fallback = 0)
{
    if (hasKey(record, primary)) {
        return intValue(record, primary, fallback);
    }
    if (hasKey(record, "vehicle_id")) {
        return intValue(record, "vehicle_id", fallback);
    }
    if (hasKey(record, "vid")) {
        return intValue(record, "vid", fallback);
    }
    if (hasKey(record, "ID")) {
        return intValue(record, "ID", fallback);
    }
    if (hasKey(record, "id")) {
        return intValue(record, "id", fallback);
    }
    return fallback;
}

double distanceM(const json& left, const json& right)
{
    const double dx = numberValue(left, "x") - numberValue(right, "x");
    const double dy = numberValue(left, "y") - numberValue(right, "y");
    const double dz = numberValue(left, "z") - numberValue(right, "z");
    return std::sqrt(dx * dx + dy * dy + dz * dz);
}

bool sendAll(int fd, const std::string& data)
{
    const char* buffer = data.data();
    std::size_t remaining = data.size();
    while (remaining > 0) {
        ssize_t written = ::send(fd, buffer, remaining, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        buffer += written;
        remaining -= static_cast<std::size_t>(written);
    }
    return true;
}

bool recvLine(int fd, std::string& line)
{
    line.clear();
    char ch = 0;
    while (true) {
        ssize_t readCount = ::recv(fd, &ch, 1, 0);
        if (readCount < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (readCount == 0) {
            return false;
        }
        if (ch == '\n') {
            return true;
        }
        if (ch != '\r') {
            line.push_back(ch);
        }
    }
}

} // namespace

class MetsrVeinsBridge : public omnetpp::cSimpleModule {
  private:
    std::string bridgeHost;
    int bridgePort = 9099;
    double baseLatencyMs = 8.0;
    double perMessageLatencyMs = 0.35;
    double perPayloadByteLatencyUs = 0.04;
    double contentionLossSlope = 0.002;
    double bitrateMbps = 6.0;
    double macSlotTimeMs = 0.013;
    double maxJitterMs = 0.25;
    double communicationRangeM = 1000.0;
    double distanceLatencyUsPerM = 2.0;
    double distanceLossAtRange = 0.05;
    int pollIntervalMs = 10;
    double defaultTickDurationS = 0.1;
    std::string bridgeBackend = "abstract_omnetpp";
    std::string backendImplementation = "abstract_event_profile";
    std::string networkModel = DEFAULT_NETWORK_MODEL;
    std::string backendNote;
    std::string radioAccess = "cv2x";
    double simu5gReceiveTimeoutMs = 1000.0;
    std::string simu5gVehicleModuleName = "car";
    std::string simu5gMobilityModuleName = "mobility";
    int simu5gBsmAppIndex = 0;
    int simu5gLocalPort = 4400;
    int simu5gDestPort = 4400;
    bool simu5gUpdateMobility = true;
    double veinsReceiveTimeoutMs = 1000.0;
    std::string veinsVehicleModuleType = "MetsrVeinsVehicle";
    double veinsCoordinateOffsetX = 10000.0;
    double veinsCoordinateOffsetY = 10000.0;
    double veinsCoordinateOffsetZ = 0.0;
    bool veinsFlipY = false;
    int veinsMaxVehicles = 4096;
    bool logRequests = true;

    std::thread serverThread;
    std::atomic<bool> running {false};
    int serverFd = -1;
    int activeClientFd = -1;
    std::mutex socketMutex;
    std::mutex pendingMutex;
    std::deque<std::shared_ptr<SyncWork>> pendingSyncRequests;
    std::map<std::string, Simu5gPendingDelivery> pendingSimu5gDeliveries;
    std::map<std::string, Simu5gPc5PendingTransmission> pendingSimu5gPc5Transmissions;
    std::map<int, int> simu5gVehicleIndexById;
    std::set<int> allocatedSimu5gVehicleIndexes;
    std::map<std::string, VeinsPendingTransmission> pendingVeinsTransmissions;
    std::map<int, omnetpp::cModule*> veinsVehicleById;
    std::set<
        std::shared_ptr<SyncWork>,
        std::owner_less<std::shared_ptr<SyncWork>>> activeSyncWorkSet;
    int activeSyncWorks = 0;
    omnetpp::cMessage* keepAlive = nullptr;

  public:
    ~MetsrVeinsBridge() override;

  protected:
    void initialize() override;
    void handleMessage(omnetpp::cMessage* message) override;
    void finish() override;

  private:
    void serverLoop();
    void handleConnection(int clientFd);
    json handleRequest(const json& request);
    json submitSyncTick(const json& request);
    void drainPendingSyncRequests();
    void startSyncWork(const std::shared_ptr<SyncWork>& work);
    void handleSyncTickBoundary(omnetpp::cMessage* message);
    void expireSyncWorkDeliveries(const std::shared_ptr<SyncWork>& work);
    void recordAttackEvents(const std::shared_ptr<SyncWork>& work);
    bool runActiveBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    void runAbstractEventBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    bool runSimu5gCellularUuBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    bool updateSimu5gMobility(int vehicleIndex, int vehicleId, const json& vehicle);
    bool injectSimu5gBsm(
        const std::shared_ptr<SyncWork>& work,
        const json& message,
        const json& metric,
        const std::string& transportMessageId,
        int senderIndex,
        int receiverIndex,
        double generationTimeS);
    bool runSimu5gPc5Backend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    bool injectSimu5gPc5Transmission(
        Simu5gPc5PendingTransmission& pending,
        int senderIndex);
    void markSimu5gPc5Drop(
        Simu5gPc5PendingTransmission& pending,
        int receiverIndex,
        const std::string& dropReason);
    bool runVeins80211pBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    omnetpp::cModule* ensureVeinsVehicle(int vehicleId, const json& vehicle);
    bool updateVeinsMobility(int vehicleId, const json& vehicle);
    bool injectVeinsTransmission(
        VeinsPendingTransmission& pending,
        int senderId,
        long recipientMac);
    void handleVeinsReport(omnetpp::cMessage* message);
    void handleVeinsTimeout(omnetpp::cMessage* message);
    void markVeinsDrop(
        VeinsPendingTransmission& pending,
        int receiverId,
        const std::string& dropReason);
    omnetpp::cModule* simu5gVehicleModule(int vehicleIndex) const;
    int ensureSimu5gVehicleIndex(int vehicleId);
    void handleSimu5gReport(omnetpp::cMessage* message);
    void handleSimu5gTimeout(omnetpp::cMessage* message);
    void markSimu5gDrop(
        const Simu5gPendingDelivery& delivery,
        const std::string& dropReason);
    void handlePacketDelivery(omnetpp::cMessage* message);
    void completeSyncWork(
        const std::shared_ptr<SyncWork>& work,
        const std::string& status = "ok",
        const std::string& message = "");
    void closeServerSocket();
    json bridgeMetadata(bool includeNote = false) const;
    void addBridgeMetadata(json& record, bool includeNote = false) const;
    double scheduledLatencyMs(int receiverLoad, int queuePosition, int payloadBytes, double distanceM);
    double packetErrorRate(int receiverLoad, double distanceM) const;
    double boundedReceiveTimeoutS(
        const std::shared_ptr<SyncWork>& work,
        double configuredTimeoutMs) const;
};

Define_Module(MetsrVeinsBridge);

MetsrVeinsBridge::~MetsrVeinsBridge()
{
    running = false;
    closeServerSocket();
    if (serverThread.joinable()) {
        serverThread.join();
    }
}

void MetsrVeinsBridge::initialize()
{
    bridgeHost = par("bridgeHost").stringValue();
    bridgePort = par("bridgePort").intValue();
    baseLatencyMs = par("baseLatencyMs").doubleValue();
    perMessageLatencyMs = par("perMessageLatencyMs").doubleValue();
    perPayloadByteLatencyUs = par("perPayloadByteLatencyUs").doubleValue();
    contentionLossSlope = par("contentionLossSlope").doubleValue();
    bitrateMbps = par("bitrateMbps").doubleValue();
    macSlotTimeMs = par("macSlotTimeMs").doubleValue();
    maxJitterMs = par("maxJitterMs").doubleValue();
    communicationRangeM = par("communicationRangeM").doubleValue();
    distanceLatencyUsPerM = par("distanceLatencyUsPerM").doubleValue();
    distanceLossAtRange = par("distanceLossAtRange").doubleValue();
    pollIntervalMs = par("pollIntervalMs").intValue();
    defaultTickDurationS = par("defaultTickDurationS").doubleValue();
    bridgeBackend = par("bridgeBackend").stringValue();
    backendImplementation = par("backendImplementation").stringValue();
    networkModel = par("networkModel").stringValue();
    backendNote = par("backendNote").stringValue();
    radioAccess = par("radioAccess").stringValue();
    simu5gReceiveTimeoutMs = par("simu5gReceiveTimeoutMs").doubleValue();
    simu5gVehicleModuleName = par("simu5gVehicleModuleName").stringValue();
    simu5gMobilityModuleName = par("simu5gMobilityModuleName").stringValue();
    simu5gBsmAppIndex = par("simu5gBsmAppIndex").intValue();
    simu5gLocalPort = par("simu5gLocalPort").intValue();
    simu5gDestPort = par("simu5gDestPort").intValue();
    simu5gUpdateMobility = par("simu5gUpdateMobility").boolValue();
    veinsReceiveTimeoutMs = par("veinsReceiveTimeoutMs").doubleValue();
    veinsVehicleModuleType = par("veinsVehicleModuleType").stringValue();
    veinsCoordinateOffsetX = par("veinsCoordinateOffsetX").doubleValue();
    veinsCoordinateOffsetY = par("veinsCoordinateOffsetY").doubleValue();
    veinsCoordinateOffsetZ = par("veinsCoordinateOffsetZ").doubleValue();
    veinsFlipY = par("veinsFlipY").boolValue();
    veinsMaxVehicles = par("veinsMaxVehicles").intValue();
    logRequests = par("logRequests").boolValue();

    running = true;
    keepAlive = new omnetpp::cMessage("bridge-keep-alive");
    keepAlive->setKind(KIND_KEEP_ALIVE);
    // Poll external requests without allowing an idle sidecar to advance model
    // time. Active radio events advance time only to the sync-tick boundary.
    scheduleAt(omnetpp::simTime(), keepAlive);
    serverThread = std::thread(&MetsrVeinsBridge::serverLoop, this);
    EV_INFO << "METS-R Veins bridge starting on " << bridgeHost << ":" << bridgePort
            << " backend=" << bridgeBackend
            << " implementation=" << backendImplementation
            << " radio_access=" << radioAccess << "\n";
}

void MetsrVeinsBridge::handleMessage(omnetpp::cMessage* message)
{
    if (message == keepAlive) {
        drainPendingSyncRequests();
        if (running && activeSyncWorks == 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::max(1, pollIntervalMs)));
        }
        if (running) {
            const double delayS = activeSyncWorks == 0
                ? 0.0
                : std::max(1, pollIntervalMs) / 1000.0;
            scheduleAt(omnetpp::simTime() + delayS, keepAlive);
        }
        return;
    }

    if (message->getKind() == KIND_SYNC_TICK_BOUNDARY) {
        handleSyncTickBoundary(message);
        return;
    }

    if (message->getKind() == KIND_PACKET_DELIVERY) {
        handlePacketDelivery(message);
        return;
    }

    if (message->getKind() == KIND_SIMU5G_RX_REPORT) {
        handleSimu5gReport(message);
        return;
    }

    if (message->getKind() == KIND_SIMU5G_SYNC_TIMEOUT) {
        handleSimu5gTimeout(message);
        return;
    }

    if (message->getKind() == KIND_VEINS_RX_REPORT) {
        handleVeinsReport(message);
        return;
    }

    if (message->getKind() == KIND_VEINS_SYNC_TIMEOUT) {
        handleVeinsTimeout(message);
        return;
    }

    delete message;
}

void MetsrVeinsBridge::finish()
{
    running = false;
    {
        std::deque<std::shared_ptr<SyncWork>> pending;
        {
            std::lock_guard<std::mutex> lock(pendingMutex);
            pending.swap(pendingSyncRequests);
        }
        for (const auto& work : pending) {
            std::lock_guard<std::mutex> workLock(work->mutex);
            work->response = {
                {"type", "sync_tick_result"},
                {"request_id", work->requestId},
                {"status", "error"},
                {"message", "bridge stopped before the request reached the OMNeT++ event loop"},
            };
            work->done = true;
            work->cv.notify_all();
        }
    }
    const std::vector<std::shared_ptr<SyncWork>> activeWorks(
        activeSyncWorkSet.begin(),
        activeSyncWorkSet.end());
    for (const auto& work : activeWorks) {
        completeSyncWork(
            work,
            "error",
            "bridge stopped before the sync_tick boundary");
    }
    closeServerSocket();
    if (serverThread.joinable()) {
        serverThread.join();
    }
    if (keepAlive != nullptr) {
        cancelAndDelete(keepAlive);
        keepAlive = nullptr;
    }
    for (auto& item : pendingSimu5gDeliveries) {
        if (item.second.work && item.second.work->timeoutEvent != nullptr) {
            auto* holder = static_cast<SyncWorkHolder*>(item.second.work->timeoutEvent->getContextPointer());
            delete holder;
            item.second.work->timeoutEvent->setContextPointer(nullptr);
            cancelAndDelete(item.second.work->timeoutEvent);
            item.second.work->timeoutEvent = nullptr;
        }
    }
    pendingSimu5gDeliveries.clear();
    for (auto& item : pendingSimu5gPc5Transmissions) {
        if (item.second.work && item.second.work->timeoutEvent != nullptr) {
            auto* holder = static_cast<SyncWorkHolder*>(
                item.second.work->timeoutEvent->getContextPointer());
            delete holder;
            item.second.work->timeoutEvent->setContextPointer(nullptr);
            cancelAndDelete(item.second.work->timeoutEvent);
            item.second.work->timeoutEvent = nullptr;
        }
    }
    pendingSimu5gPc5Transmissions.clear();
    simu5gVehicleIndexById.clear();
    allocatedSimu5gVehicleIndexes.clear();
    for (auto& item : pendingVeinsTransmissions) {
        if (item.second.work && item.second.work->timeoutEvent != nullptr) {
            auto* holder = static_cast<SyncWorkHolder*>(
                item.second.work->timeoutEvent->getContextPointer());
            delete holder;
            item.second.work->timeoutEvent->setContextPointer(nullptr);
            cancelAndDelete(item.second.work->timeoutEvent);
            item.second.work->timeoutEvent = nullptr;
        }
    }
    pendingVeinsTransmissions.clear();
    veinsVehicleById.clear();
}

void MetsrVeinsBridge::closeServerSocket()
{
    int fdToClose = -1;
    int clientToClose = -1;
    {
        std::lock_guard<std::mutex> lock(socketMutex);
        fdToClose = serverFd;
        serverFd = -1;
        clientToClose = activeClientFd;
        activeClientFd = -1;
    }
    if (fdToClose >= 0) {
        ::shutdown(fdToClose, SHUT_RDWR);
        ::close(fdToClose);
    }
    if (clientToClose >= 0) {
        ::shutdown(clientToClose, SHUT_RDWR);
        ::close(clientToClose);
    }
}

json MetsrVeinsBridge::bridgeMetadata(bool includeNote) const
{
    json metadata = {
        {"bridge_backend", bridgeBackend},
        {"backend_implementation", backendImplementation},
        {"bridge_model", networkModel},
        {"network_model", networkModel},
        {"radio_access", radioAccess},
    };
    if (includeNote && !backendNote.empty()) {
        metadata["backend_note"] = backendNote;
    }
    return metadata;
}

void MetsrVeinsBridge::addBridgeMetadata(json& record, bool includeNote) const
{
    record["bridge_backend"] = bridgeBackend;
    record["backend_implementation"] = backendImplementation;
    record["bridge_model"] = networkModel;
    record["network_model"] = networkModel;
    record["radio_access"] = radioAccess;
    if (includeNote && !backendNote.empty()) {
        record["backend_note"] = backendNote;
    }
}

void MetsrVeinsBridge::serverLoop()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "METS-R Veins bridge: failed to create socket: "
                  << std::strerror(errno) << "\n";
        return;
    }

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(bridgePort));
    if (bridgeHost == "0.0.0.0" || bridgeHost.empty()) {
        address.sin_addr.s_addr = INADDR_ANY;
    }
    else if (::inet_pton(AF_INET, bridgeHost.c_str(), &address.sin_addr) != 1) {
        std::cerr << "METS-R Veins bridge: invalid bridgeHost " << bridgeHost << "\n";
        ::close(fd);
        return;
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        std::cerr << "METS-R Veins bridge: failed to bind " << bridgeHost << ":"
                  << bridgePort << ": " << std::strerror(errno) << "\n";
        ::close(fd);
        return;
    }

    if (::listen(fd, 8) < 0) {
        std::cerr << "METS-R Veins bridge: listen failed: "
                  << std::strerror(errno) << "\n";
        ::close(fd);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(socketMutex);
        serverFd = fd;
    }

    std::cout << "METS-R Veins bridge listening on " << bridgeHost << ":"
              << bridgePort << std::endl;

    while (running) {
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(fd, &readSet);
        timeval timeout {};
        timeout.tv_sec = 0;
        timeout.tv_usec = 250000;
        int ready = ::select(fd + 1, &readSet, nullptr, nullptr, &timeout);
        if (!running) {
            break;
        }
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0 || !FD_ISSET(fd, &readSet)) {
            continue;
        }

        sockaddr_in clientAddress {};
        socklen_t clientLength = sizeof(clientAddress);
        int clientFd = ::accept(fd, reinterpret_cast<sockaddr*>(&clientAddress), &clientLength);
        if (clientFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (running) {
                std::cerr << "METS-R Veins bridge: accept failed: "
                          << std::strerror(errno) << "\n";
            }
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(socketMutex);
            activeClientFd = clientFd;
        }
        handleConnection(clientFd);
        bool shouldCloseClient = true;
        {
            std::lock_guard<std::mutex> lock(socketMutex);
            if (activeClientFd == clientFd) {
                activeClientFd = -1;
            }
            else {
                shouldCloseClient = false;
            }
        }
        if (shouldCloseClient) {
            ::close(clientFd);
        }
    }

    closeServerSocket();
}

void MetsrVeinsBridge::handleConnection(int clientFd)
{
    std::string line;
    while (running && recvLine(clientFd, line)) {
        json response;
        try {
            json request = json::parse(line);
            if (logRequests) {
                const std::string type = stringValue(request, "type");
                std::cout << "METS-R Veins bridge request type=" << type
                          << " request_id=" << intValue(request, "request_id", 0);
                if (type == "sync_tick") {
                    std::cout << " tick=" << intValue(request, "tick", 0)
                              << " vehicles=" << request.value("vehicles", json::array()).size()
                              << " bsm_messages=" << request.value("bsm_messages", json::array()).size();
                }
                std::cout << std::endl;
            }
            response = handleRequest(request);
        }
        catch (const std::exception& exc) {
            response = {
                {"type", "error"},
                {"status", "error"},
                {"message", exc.what()},
            };
        }

        std::string raw = response.dump() + "\n";
        if (!sendAll(clientFd, raw)) {
            break;
        }
    }
}

json MetsrVeinsBridge::handleRequest(const json& request)
{
    const std::string type = stringValue(request, "type");
    const int requestId = intValue(request, "request_id", 0);

    if (type == "hello") {
        json response = {
            {"type", "hello_result"},
            {"request_id", requestId},
            {"status", "ok"},
            {"protocol", PROTOCOL_NAME},
            {"version", PROTOCOL_VERSION},
        };
        addBridgeMetadata(response, true);
        return response;
    }
    if (type == "ping") {
        return {
            {"type", "ping_result"},
            {"request_id", requestId},
            {"status", "ok"},
        };
    }
    if (type == "reset") {
        json response = {
            {"type", "reset_result"},
            {"request_id", requestId},
            {"status", "error"},
            {"message",
             "The bridge cannot rewind OMNeT++ simulation time and radio protocol state in place; stop and restart the selected sidecar for a clean experiment."},
        };
        addBridgeMetadata(response, true);
        return response;
    }
    if (type == "sync_tick") {
        return submitSyncTick(request);
    }

    return {
        {"type", type + "_result"},
        {"request_id", requestId},
        {"status", "error"},
        {"message", "unsupported request type: " + type},
    };
}

json MetsrVeinsBridge::submitSyncTick(const json& request)
{
    auto work = std::make_shared<SyncWork>();
    work->requestId = intValue(request, "request_id", 0);
    work->tick = intValue(request, "tick", 0);
    work->request = request;

    if (!running) {
        return {
            {"type", "sync_tick_result"},
            {"request_id", work->requestId},
            {"status", "error"},
            {"message", "bridge is not running"},
        };
    }

    {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pendingSyncRequests.push_back(work);
    }

    std::unique_lock<std::mutex> lock(work->mutex);
    work->cv.wait(lock, [&work]() { return work->done; });
    return work->response;
}

void MetsrVeinsBridge::drainPendingSyncRequests()
{
    std::deque<std::shared_ptr<SyncWork>> pending;
    {
        std::lock_guard<std::mutex> lock(pendingMutex);
        pending.swap(pendingSyncRequests);
    }

    for (const auto& work : pending) {
        startSyncWork(work);
    }
}

void MetsrVeinsBridge::startSyncWork(const std::shared_ptr<SyncWork>& work)
{
    activeSyncWorks += 1;
    activeSyncWorkSet.insert(work);
    work->durationS = numberValue(
        work->request,
        "duration_s",
        defaultTickDurationS);
    if (!std::isfinite(work->durationS) || work->durationS <= 0.0) {
        completeSyncWork(
            work,
            "error",
            "sync_tick duration_s must be a finite positive number");
        return;
    }
    work->tickStartTime = omnetpp::simTime();
    work->tickEndTime = work->tickStartTime + work->durationS;
    const auto vehicles = work->request.value("vehicles", json::array());
    const auto messages = work->request.value("bsm_messages", json::array());

    recordAttackEvents(work);
    if (!runActiveBackend(work, vehicles, messages)) {
        return;
    }

    auto* boundary = new omnetpp::cMessage(
        "syncTickBoundary",
        KIND_SYNC_TICK_BOUNDARY);
    boundary->setContextPointer(new SyncWorkHolder {work});
    work->boundaryEvent = boundary;
    scheduleAt(work->tickEndTime, boundary);

    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
}

double MetsrVeinsBridge::boundedReceiveTimeoutS(
    const std::shared_ptr<SyncWork>& work,
    double configuredTimeoutMs) const
{
    const double remainingS = std::max(
        0.0,
        (work->tickEndTime - omnetpp::simTime()).dbl());
    const double configuredS = std::max(0.0, configuredTimeoutMs / 1000.0);
    return std::max(1e-9, std::min(configuredS, remainingS));
}

void MetsrVeinsBridge::recordAttackEvents(const std::shared_ptr<SyncWork>& work)
{
    const auto attacks = work->request.value("attacks", json::array());
    for (const auto& attack : attacks) {
        json event = attack;
        event["tick"] = work->tick;
        event["status"] = "submitted";
        addBridgeMetadata(event);
        work->attackEvents.push_back(event);
    }
}

bool MetsrVeinsBridge::runActiveBackend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
    if (backendImplementation == "abstract_event_profile" ||
        backendImplementation == "abstract_profile_pending_full_veins") {
        runAbstractEventBackend(work, vehicles, messages);
        return true;
    }

    if (backendImplementation == "simu5g_cellular_uu") {
        return runSimu5gCellularUuBackend(work, vehicles, messages);
    }

    if (backendImplementation == "simu5g_lte_d2d_pc5" ||
        backendImplementation == "simu5g_nr_d2d_pc5") {
        return runSimu5gPc5Backend(work, vehicles, messages);
    }

    if (backendImplementation == "veins_80211p_phy_mac") {
        return runVeins80211pBackend(work, vehicles, messages);
    }

    json errorEvent = {
        {"tick", work->tick},
        {"status", "error"},
        {"message", "bridge backend implementation is not available in this build"},
        {"requested_backend", bridgeBackend},
        {"requested_backend_implementation", backendImplementation},
    };
    addBridgeMetadata(errorEvent, true);
    work->attackEvents.push_back(errorEvent);
    completeSyncWork(work, "error", errorEvent["message"].get<std::string>());
    return false;
}

omnetpp::cModule* MetsrVeinsBridge::ensureVeinsVehicle(int vehicleId, const json& vehicle)
{
#ifndef METSR_WITH_VEINS
    (void) vehicleId;
    (void) vehicle;
    return nullptr;
#else
    auto existing = veinsVehicleById.find(vehicleId);
    if (existing != veinsVehicleById.end()) {
        return existing->second;
    }
    if (static_cast<int>(veinsVehicleById.size()) >= veinsMaxVehicles) {
        throw omnetpp::cRuntimeError("veinsMaxVehicles=%d exceeded", veinsMaxVehicles);
    }
    omnetpp::cModuleType* moduleType =
        omnetpp::cModuleType::get(veinsVehicleModuleType.c_str());
    if (moduleType == nullptr) {
        throw omnetpp::cRuntimeError(
            "Veins vehicle module type '%s' was not found",
            veinsVehicleModuleType.c_str());
    }
    const std::string moduleName = "metsrVehicle_" + std::to_string(vehicleId);
    omnetpp::cModule* node = moduleType->create(moduleName.c_str(), getParentModule());
    node->par("externalVehicleId").setIntValue(vehicleId);
    node->finalizeParameters();
    node->buildInside();

    omnetpp::cModule* mobility = node->getSubmodule("veinsmobility");
    const double rawY = numberValue(vehicle, "y");
    mobility->par("x").setDoubleValue(numberValue(vehicle, "x") + veinsCoordinateOffsetX);
    mobility->par("y").setDoubleValue(
        (veinsFlipY ? -rawY : rawY) + veinsCoordinateOffsetY);
    mobility->par("z").setDoubleValue(numberValue(vehicle, "z") + veinsCoordinateOffsetZ);
    node->scheduleStart(omnetpp::simTime());
    node->callInitialize();
    veinsVehicleById[vehicleId] = node;
    return node;
#endif
}

bool MetsrVeinsBridge::updateVeinsMobility(int vehicleId, const json& vehicle)
{
    omnetpp::cModule* node = ensureVeinsVehicle(vehicleId, vehicle);
    if (node == nullptr) {
        return false;
    }
    omnetpp::cModule* mobility = node->getSubmodule("veinsmobility");
    if (mobility == nullptr || !mobility->hasGate(VEINS_BRIDGE_GATE)) {
        return false;
    }
    const double rawY = numberValue(vehicle, "y");
    auto* update = new omnetpp::cMessage(
        "metsrVeinsMobilityUpdate",
        KIND_VEINS_MOBILITY_UPDATE);
    update->addPar("vehicle_id") = vehicleId;
    update->addPar("x") = numberValue(vehicle, "x") + veinsCoordinateOffsetX;
    update->addPar("y") = (veinsFlipY ? -rawY : rawY) + veinsCoordinateOffsetY;
    update->addPar("z") = numberValue(vehicle, "z") + veinsCoordinateOffsetZ;
    update->addPar("speed_mps") =
        numberValue(vehicle, "speed_mps", numberValue(vehicle, "speed"));
    double heading = numberValue(vehicle, "heading_deg", numberValue(vehicle, "heading"));
    if (veinsFlipY) {
        heading = -heading;
    }
    update->addPar("heading_deg") = heading;
    sendDirect(update, mobility, VEINS_BRIDGE_GATE);
    return true;
}

bool MetsrVeinsBridge::injectVeinsTransmission(
    VeinsPendingTransmission& pending,
    int senderId,
    long recipientMac)
{
    auto senderIt = veinsVehicleById.find(senderId);
    if (senderIt == veinsVehicleById.end()) {
        return false;
    }
    omnetpp::cModule* app = senderIt->second->getSubmodule("appl");
    if (app == nullptr || !app->hasGate(VEINS_BRIDGE_GATE)) {
        return false;
    }
    const json& message = pending.message;
    auto* tx = new omnetpp::cMessage("metsrVeinsBsmTx", KIND_VEINS_BSM_REQUEST);
    tx->addPar("transmission_id") = pending.transmissionId.c_str();
    tx->addPar("message_id") = stringValue(message, "message_id").c_str();
    tx->addPar("message_name") = bsmMessageName(message).c_str();
    tx->addPar("message_standard") = bsmMessageStandard(message).c_str();
    tx->addPar("expected_receiver_ids") =
        joinReceiverIds(pending.expectedReceivers).c_str();
    tx->addPar("wire_data_b64") = bsmWireDataB64(message).c_str();
    tx->addPar("payload_encoding") = bsmPayloadEncoding(message).c_str();
    tx->addPar("content") = bsmContent(message).c_str();
    tx->addPar("attack_id") =
        bsmAttackString(message, "attack_id", "attackId").c_str();
    tx->addPar("attack_type") =
        bsmAttackString(message, "attack_type", "attackType").c_str();
    tx->addPar("tick") = pending.work->tick;
    tx->addPar("sender_id") = senderId;
    tx->addPar("receiver_id") = recipientMac < 0
        ? -1
        : *pending.expectedReceivers.begin();
    tx->addPar("message_count") = bsmMessageCount(message);
    tx->addPar("payload_bytes") = bsmPayloadBytes(message);
    tx->addPar("payload_bit_length") = bsmPayloadBitLength(message);
    tx->addPar("attacked") =
        bsmAttackValue(message, "attacked", "attacked", json(false)).get<bool>() ? 1 : 0;
    tx->addPar("tx_time_s") = bsmTxTimeS(message, pending.generationTimeS);
    tx->addPar("recipient_mac") = recipientMac;
    sendDirect(tx, app, VEINS_BRIDGE_GATE);
    return true;
}

bool MetsrVeinsBridge::runVeins80211pBackend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
#ifndef METSR_WITH_VEINS
    completeSyncWork(
        work,
        "error",
        "veins_80211p_phy_mac requires a bridge built with build_veins.sh");
    return false;
#else
    std::set<int> currentVehicleIds;
    for (const auto& vehicle : vehicles) {
        const int vehicleId =
            entityId(vehicle, "vehicle_id", entityId(vehicle, "id", 0));
        if (vehicleId > 0) {
            currentVehicleIds.insert(vehicleId);
        }
    }
    for (auto it = veinsVehicleById.begin();
         it != veinsVehicleById.end();) {
        if (currentVehicleIds.find(it->first) != currentVehicleIds.end()) {
            ++it;
            continue;
        }
        omnetpp::cModule* node = it->second;
        omnetpp::cModule* nic =
            node != nullptr ? node->getSubmodule("nic") : nullptr;
        auto* connectionManager =
            dynamic_cast<veins::BaseConnectionManager*>(
                getParentModule()->getSubmodule("connectionManager"));
        if (node == nullptr || nic == nullptr || connectionManager == nullptr) {
            throw omnetpp::cRuntimeError(
                "Could not safely remove departed Veins vehicle %d",
                it->first);
        }
        // Match Veins' TraCIScenarioManager deletion order so the connection
        // manager never retains a pointer to a deleted radio.
        connectionManager->unregisterNic(nic);
        node->callFinish();
        node->deleteModule();
        it = veinsVehicleById.erase(it);
    }

    std::map<int, json> vehicleById;
    for (const auto& vehicle : vehicles) {
        const int vehicleId = entityId(vehicle, "vehicle_id", entityId(vehicle, "id", 0));
        if (vehicleId <= 0) {
            continue;
        }
        vehicleById[vehicleId] = vehicle;
        if (!updateVeinsMobility(vehicleId, vehicle)) {
            throw omnetpp::cRuntimeError(
                "Could not create/update Veins vehicle %d",
                vehicleId);
        }
    }

    auto addImmediateVeinsDrop = [&](const json& message,
                                     int senderId,
                                     int receiverId,
                                     const std::string& reason) {
        const std::string sourceMessageId = messageIdValue(
            message, work->tick, senderId, receiverId);
        json metric = {
            {"tick", work->tick},
            {"message_id", sourceMessageId},
            {"source_message_id", sourceMessageId},
            {"transmission_id", nullptr},
            {"tx_time_s", bsmTxTimeS(
                message, omnetpp::simTime().dbl())},
            {"sender_id", senderId},
            {"receiver_id", receiverId > 0
                ? json(receiverId)
                : json(nullptr)},
            {"message_name", bsmMessageName(message)},
            {"message_standard", bsmMessageStandard(message)},
            {"message_count", bsmMessageCount(message)},
            {"payload_bytes", bsmPayloadBytes(message)},
            {"payload_bit_length", bsmPayloadBitLength(message)},
            {"payload_encoding", bsmPayloadEncoding(message)},
            {"wire_payload_present", !bsmWireDataB64(message).empty()},
            {"radio_mode", "802.11p"},
            {"backend_time_source", "veins_phy_mac_receive_event"},
            {"physical_transmission_count", 0},
            {"delivered", false},
            {"drop_reason", reason},
            {"rx_time_s", nullptr},
            {"latency_ms", nullptr},
        };
        addBridgeMetadata(metric);
        work->metrics.push_back(std::move(metric));
    };

    std::vector<VeinsPendingTransmission> transmissions;
    std::map<std::string, std::size_t> legacyGroupIndex;
    for (const auto& message : messages) {
        const int senderId = bsmSenderId(message);
        const int explicitReceiver = bsmReceiverId(message);
        const std::string wirePayloadError =
            bsmWirePayloadValidationError(message);
        if (!wirePayloadError.empty()) {
            completeSyncWork(
                work,
                "error",
                "veins_80211p_phy_mac rejected wire payload: " +
                    wirePayloadError);
            return false;
        }

        const auto expectedIt = message.find("expected_receiver_ids");
        const bool hasExpectedReceiverList =
            expectedIt != message.end();
        if (hasExpectedReceiverList && !expectedIt->is_array()) {
            addImmediateVeinsDrop(
                message,
                senderId,
                explicitReceiver,
                "invalid_expected_receiver_ids");
            continue;
        }

        std::set<int> requestedReceivers;
        for (const int receiverId : explicitExpectedReceivers(message)) {
            requestedReceivers.insert(receiverId);
        }
        if (!hasExpectedReceiverList && explicitReceiver > 0) {
            requestedReceivers.insert(explicitReceiver);
        }
        if (!hasExpectedReceiverList && explicitReceiver <= 0) {
            for (const auto& item : vehicleById) {
                if (item.first != senderId) {
                    requestedReceivers.insert(item.first);
                }
            }
        }
        if (hasExpectedReceiverList && requestedReceivers.empty()) {
            addImmediateVeinsDrop(
                message,
                senderId,
                explicitReceiver,
                "no_expected_receivers");
            continue;
        }

        if (senderId <= 0 ||
            vehicleById.find(senderId) == vehicleById.end()) {
            if (requestedReceivers.empty()) {
                addImmediateVeinsDrop(
                    message,
                    senderId,
                    explicitReceiver,
                    "unknown_sender_vehicle");
            }
            else {
                for (const int receiverId : requestedReceivers) {
                    addImmediateVeinsDrop(
                        message,
                        senderId,
                        receiverId,
                        "unknown_sender_vehicle");
                }
            }
            continue;
        }

        std::set<int> receivers;
        for (const int receiverId : requestedReceivers) {
            if (receiverId == senderId) {
                addImmediateVeinsDrop(
                    message,
                    senderId,
                    receiverId,
                    "receiver_is_sender");
                continue;
            }
            if (vehicleById.find(receiverId) == vehicleById.end()) {
                addImmediateVeinsDrop(
                    message,
                    senderId,
                    receiverId,
                    "unknown_receiver_vehicle");
                continue;
            }
            receivers.insert(receiverId);
        }
        if (receivers.empty()) {
            continue;
        }

        // Legacy tutorials emitted one identical BSM per receiver. Consolidate
        // those into one physical broadcast but retain each legacy row for
        // result matching. Synthetic load messages remain distinct sends.
        const bool legacyPair =
            explicitReceiver > 0 &&
            !hasExpectedReceiverList &&
            bsmMessageName(message) == "BasicSafetyMessage";
        std::ostringstream keyBuilder;
        keyBuilder << work->tick << "|" << senderId << "|"
                   << bsmMessageName(message) << "|"
                   << bsmMessageStandard(message) << "|"
                   << bsmContent(message) << "|"
                   << bsmTxTimeS(message, -1) << "|"
                   << bsmMessageCount(message) << "|"
                   << bsmPayloadBytes(message) << "|"
                   << bsmPayloadBitLength(message) << "|"
                   << bsmPayloadEncoding(message) << "|"
                   << bsmWireDataB64(message) << "|"
                   << bsmAttackString(message, "attack_id", "attackId") << "|"
                   << bsmAttackString(message, "attack_type", "attackType") << "|"
                   << stringValue(message, "x") << "|"
                   << stringValue(message, "y") << "|"
                   << stringValue(message, "z") << "|"
                   << stringValue(message, "speed_mps") << "|"
                   << stringValue(message, "heading_deg");
        const std::string groupKey = legacyPair
            ? keyBuilder.str()
            : messageIdValue(message, work->tick, senderId, explicitReceiver);

        std::size_t index = transmissions.size();
        auto groupIt = legacyGroupIndex.find(groupKey);
        bool overlapsLegacyGroup = false;
        if (legacyPair && groupIt != legacyGroupIndex.end()) {
            for (const int receiverId : receivers) {
                if (transmissions[groupIt->second]
                        .expectedReceivers.count(receiverId) != 0) {
                    overlapsLegacyGroup = true;
                    break;
                }
            }
        }
        if (legacyPair && groupIt != legacyGroupIndex.end() &&
            !overlapsLegacyGroup) {
            index = groupIt->second;
        }
        else {
            VeinsPendingTransmission pending;
            pending.work = work;
            pending.message = message;
            pending.generationTimeS = omnetpp::simTime().dbl();
            pending.transmissionId =
                "veins:" + std::to_string(work->requestId) + ":" +
                std::to_string(work->tick) + ":" +
                std::to_string(senderId) + ":" +
                std::to_string(transmissions.size());
            pending.metric = {
                {"tick", work->tick},
                {"transmission_id", pending.transmissionId},
                {"source_message_id", messageIdValue(
                    message, work->tick, senderId, explicitReceiver)},
                {"sender_id", senderId},
                {"message_name", bsmMessageName(message)},
                {"message_standard", bsmMessageStandard(message)},
                {"message_count", bsmMessageCount(message)},
                {"payload_bytes", bsmPayloadBytes(message)},
                {"payload_bit_length", bsmPayloadBitLength(message)},
                {"payload_encoding", bsmPayloadEncoding(message)},
                {"radio_mode", "802.11p"},
                {"backend_time_source", "veins_phy_mac_receive_event"},
                {"physical_transmission_count", 1},
                {"legacy_pairwise_consolidated", legacyPair},
                {"attacked", bsmAttackValue(message, "attacked", "attacked", json(false))},
                {"attack_id", bsmAttackString(message, "attack_id", "attackId")},
                {"attack_type", bsmAttackString(message, "attack_type", "attackType")},
            };
            addBridgeMetadata(pending.metric);
            transmissions.push_back(std::move(pending));
            index = transmissions.size() - 1;
            if (legacyPair) {
                legacyGroupIndex[groupKey] = index;
            }
        }
        for (const int receiverId : receivers) {
            json receiverMessage = message;
            receiverMessage["_bridge_distance_m"] =
                distanceM(vehicleById.at(senderId), vehicleById.at(receiverId));
            receiverMessage["_legacy_pair"] = legacyPair;
            transmissions[index].expectedReceivers.insert(receiverId);
            transmissions[index].messageByReceiver[receiverId] = std::move(receiverMessage);
        }
    }

    for (auto& pending : transmissions) {
        const int senderId = bsmSenderId(pending.message);
        const long recipientMac = -1;
        work->outstandingDeliveries +=
            static_cast<int>(pending.expectedReceivers.size());
        const std::string transmissionId = pending.transmissionId;
        pendingVeinsTransmissions[transmissionId] = std::move(pending);
        auto& stored = pendingVeinsTransmissions.at(transmissionId);
        if (!injectVeinsTransmission(stored, senderId, recipientMac)) {
            for (const int receiverId : stored.expectedReceivers) {
                markVeinsDrop(stored, receiverId, "veins_sender_app_missing");
            }
            pendingVeinsTransmissions.erase(transmissionId);
        }
    }

    if (work->outstandingDeliveries > 0) {
        auto* timeout = new omnetpp::cMessage(
            "veinsSyncTimeout",
            KIND_VEINS_SYNC_TIMEOUT);
        timeout->setContextPointer(new SyncWorkHolder {work});
        work->timeoutEvent = timeout;
        scheduleAt(
            omnetpp::simTime() + boundedReceiveTimeoutS(work, veinsReceiveTimeoutMs),
            timeout);
    }
    return true;
#endif
}

void MetsrVeinsBridge::markVeinsDrop(
    VeinsPendingTransmission& pending,
    int receiverId,
    const std::string& dropReason)
{
    if (pending.receivedReceivers.find(receiverId) != pending.receivedReceivers.end()) {
        return;
    }
    const json receiverMessage = pending.messageByReceiver.at(receiverId);
    const bool legacyPair = receiverMessage.value("_legacy_pair", false);
    json metric = pending.metric;
    metric["message_id"] = legacyPair
        ? messageIdValue(
              receiverMessage,
              pending.work->tick,
              bsmSenderId(receiverMessage),
              receiverId)
        : linkMessageId(pending.transmissionId, receiverId);
    if (legacyPair) {
        metric["source_message_id"] = metric["message_id"];
    }
    metric["receiver_id"] = receiverId;
    metric["distance_m"] = receiverMessage.value("_bridge_distance_m", 0.0);
    metric["delivered"] = false;
    metric["drop_reason"] = dropReason;
    metric["rx_time_s"] = nullptr;
    metric["latency_ms"] = nullptr;
    pending.work->metrics.push_back(metric);
    pending.receivedReceivers.insert(receiverId);
    pending.work->outstandingDeliveries =
        std::max(0, pending.work->outstandingDeliveries - 1);
}

void MetsrVeinsBridge::handleVeinsReport(omnetpp::cMessage* message)
{
    std::unique_ptr<omnetpp::cMessage> cleanup(message);
    const std::string transmissionId = message->hasPar("transmission_id")
        ? message->par("transmission_id").stringValue()
        : "";
    const int receiverId = message->hasPar("receiver_id")
        ? static_cast<int>(message->par("receiver_id").longValue())
        : 0;
    auto pendingIt = pendingVeinsTransmissions.find(transmissionId);
    if (pendingIt == pendingVeinsTransmissions.end()) {
        EV_WARN << "Ignoring Veins report for unknown transmission_id="
                << transmissionId << "\n";
        return;
    }
    VeinsPendingTransmission& pending = pendingIt->second;
    if (pending.expectedReceivers.find(receiverId) == pending.expectedReceivers.end() ||
        pending.receivedReceivers.find(receiverId) != pending.receivedReceivers.end()) {
        return;
    }

    json receiverMessage = pending.messageByReceiver.at(receiverId);
    const bool legacyPair = receiverMessage.value("_legacy_pair", false);
    const double receiveTimeS = omnetpp::simTime().dbl();
    json metric = pending.metric;
    metric["message_id"] = legacyPair
        ? messageIdValue(
              receiverMessage,
              pending.work->tick,
              bsmSenderId(receiverMessage),
              receiverId)
        : linkMessageId(transmissionId, receiverId);
    if (legacyPair) {
        metric["source_message_id"] = metric["message_id"];
    }
    metric["receiver_id"] = receiverId;
    metric["distance_m"] = receiverMessage.value("_bridge_distance_m", 0.0);
    const double bridgeTxTimeS = message->hasPar("bridge_tx_time_s")
        ? message->par("bridge_tx_time_s").doubleValue()
        : pending.generationTimeS;
    metric["application_tx_time_s"] =
        bsmTxTimeS(receiverMessage, pending.generationTimeS);
    metric["bridge_tx_time_s"] = bridgeTxTimeS;
    metric["tx_time_s"] = bridgeTxTimeS;
    metric["rx_time_s"] = receiveTimeS;
    metric["latency_ms"] = (receiveTimeS - bridgeTxTimeS) * 1000.0;
    metric["bridge_injection_delay_ms"] =
        (bridgeTxTimeS - pending.generationTimeS) * 1000.0;
    metric["bridge_end_to_end_latency_ms"] =
        (receiveTimeS - pending.generationTimeS) * 1000.0;
    const std::string expectedWireDataB64 = bsmWireDataB64(receiverMessage);
    const std::string receivedWireDataB64 = message->hasPar("wire_data_b64")
        ? message->par("wire_data_b64").stringValue()
        : "";
    const bool strictUper =
        bsmPayloadEncoding(receiverMessage) == "uper";
    const bool wirePayloadVerified =
        (!strictUper || !expectedWireDataB64.empty()) &&
        expectedWireDataB64 == receivedWireDataB64;
    metric["wire_payload_verified"] = wirePayloadVerified;
    metric["delivered"] = wirePayloadVerified;
    metric["drop_reason"] = wirePayloadVerified
        ? ""
        : (strictUper && expectedWireDataB64.empty()
              ? "missing_uper_wire_payload"
              : "wire_payload_mismatch");
    for (const char* field : {
             "sender_mac",
             "receiver_mac",
             "payload_bytes",
             "payload_bit_length"}) {
        if (message->hasPar(field)) {
            metric[field] = message->par(field).longValue();
        }
    }
    for (const char* field : {
             "snir_linear",
             "snir_db",
             "recv_power_dbm",
             "bitrate_bps"}) {
        if (message->hasPar(field)) {
            metric[field] = message->par(field).doubleValue();
        }
    }
    metric["receiver_module"] = message->hasPar("receiver_module")
        ? message->par("receiver_module").stringValue()
        : "";
    metric["payload_encoding"] = message->hasPar("payload_encoding")
        ? message->par("payload_encoding").stringValue()
        : metric.value("payload_encoding", "");
    pending.work->metrics.push_back(metric);

    if (wirePayloadVerified) {
        receiverMessage.erase("_bridge_distance_m");
        receiverMessage.erase("_legacy_pair");
        receiverMessage["source_message_id"] =
            metric["source_message_id"];
        receiverMessage["message_id"] = metric["message_id"];
        receiverMessage["transmission_id"] = transmissionId;
        receiverMessage["sender_id"] = metric["sender_id"];
        receiverMessage["receiver_id"] = receiverId;
        receiverMessage["distance_m"] = metric["distance_m"];
        receiverMessage["tx_time_s"] = metric["tx_time_s"];
        receiverMessage["rx_time_s"] = metric["rx_time_s"];
        receiverMessage["latency_ms"] = metric["latency_ms"];
        receiverMessage["bridge_injection_delay_ms"] =
            metric["bridge_injection_delay_ms"];
        receiverMessage["bridge_end_to_end_latency_ms"] =
            metric["bridge_end_to_end_latency_ms"];
        receiverMessage["payload_encoding"] = metric["payload_encoding"];
        receiverMessage["payload_bytes"] = metric["payload_bytes"];
        receiverMessage["payload_bit_length"] = metric["payload_bit_length"];
        receiverMessage["wire_payload_verified"] = true;
        addBridgeMetadata(receiverMessage);
        pending.work->received.push_back(std::move(receiverMessage));
    }

    pending.receivedReceivers.insert(receiverId);
    pending.work->outstandingDeliveries =
        std::max(0, pending.work->outstandingDeliveries - 1);
    const auto work = pending.work;
    if (pending.receivedReceivers.size() == pending.expectedReceivers.size()) {
        pendingVeinsTransmissions.erase(pendingIt);
    }
    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
}

void MetsrVeinsBridge::handleVeinsTimeout(omnetpp::cMessage* message)
{
    std::unique_ptr<omnetpp::cMessage> cleanup(message);
    auto* holder = static_cast<SyncWorkHolder*>(message->getContextPointer());
    std::shared_ptr<SyncWork> work = holder != nullptr ? holder->work : nullptr;
    delete holder;
    if (!work) {
        return;
    }
    work->timeoutEvent = nullptr;
    for (auto it = pendingVeinsTransmissions.begin();
         it != pendingVeinsTransmissions.end();) {
        if (it->second.work != work) {
            ++it;
            continue;
        }
        std::vector<int> missing;
        for (const int receiverId : it->second.expectedReceivers) {
            if (it->second.receivedReceivers.find(receiverId) ==
                it->second.receivedReceivers.end()) {
                missing.push_back(receiverId);
            }
        }
        for (const int receiverId : missing) {
            markVeinsDrop(it->second, receiverId, "veins_phy_mac_no_receive");
        }
        it = pendingVeinsTransmissions.erase(it);
    }
    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
}

omnetpp::cModule* MetsrVeinsBridge::simu5gVehicleModule(int vehicleIndex) const
{
    omnetpp::cModule* parent = getParentModule();
    if (parent == nullptr) {
        return nullptr;
    }
    return parent->getSubmodule(simu5gVehicleModuleName.c_str(), vehicleIndex);
}

int MetsrVeinsBridge::ensureSimu5gVehicleIndex(int vehicleId)
{
    const auto existing = simu5gVehicleIndexById.find(vehicleId);
    if (existing != simu5gVehicleIndexById.end()) {
        return existing->second;
    }

    // Keep a UE's RLC/MAC/PHY state attached to the same METS-R vehicle for
    // the lifetime of the run. Reusing a departed vehicle's slot would
    // silently transfer radio state to a different vehicle.
    for (int vehicleIndex = 0;
         simu5gVehicleModule(vehicleIndex) != nullptr;
         ++vehicleIndex) {
        if (allocatedSimu5gVehicleIndexes.find(vehicleIndex) !=
            allocatedSimu5gVehicleIndexes.end()) {
            continue;
        }
        simu5gVehicleIndexById[vehicleId] = vehicleIndex;
        allocatedSimu5gVehicleIndexes.insert(vehicleIndex);
        return vehicleIndex;
    }
    return -1;
}

bool MetsrVeinsBridge::updateSimu5gMobility(int vehicleIndex, int vehicleId, const json& vehicle)
{
    if (!simu5gUpdateMobility) {
        return true;
    }

    omnetpp::cModule* ue = simu5gVehicleModule(vehicleIndex);
    if (ue == nullptr) {
        return false;
    }
    omnetpp::cModule* mobility = ue->getSubmodule(simu5gMobilityModuleName.c_str());
    if (mobility == nullptr || !mobility->hasGate(SIMU5G_BRIDGE_GATE)) {
        return false;
    }

    auto* update = new omnetpp::cMessage("metsrMobilityUpdate", KIND_SIMU5G_MOBILITY_UPDATE);
    update->addPar("vehicle_id") = vehicleId;
    update->addPar("vehicle_index") = vehicleIndex;
    update->addPar("x") = numberValue(vehicle, "x");
    update->addPar("y") = numberValue(vehicle, "y");
    update->addPar("z") = numberValue(vehicle, "z");
    update->addPar("speed_mps") = numberValue(vehicle, "speed_mps", numberValue(vehicle, "speed"));
    update->addPar("heading_deg") = numberValue(vehicle, "heading_deg", numberValue(vehicle, "heading"));
    sendDirect(update, mobility, SIMU5G_BRIDGE_GATE);
    return true;
}

bool MetsrVeinsBridge::injectSimu5gBsm(
    const std::shared_ptr<SyncWork>& work,
    const json& message,
    const json& metric,
    const std::string& transportMessageId,
    int senderIndex,
    int receiverIndex,
    double generationTimeS)
{
    omnetpp::cModule* sender = simu5gVehicleModule(senderIndex);
    omnetpp::cModule* receiver = simu5gVehicleModule(receiverIndex);
    if (sender == nullptr || receiver == nullptr) {
        return false;
    }
    omnetpp::cModule* app = sender->getSubmodule("app", simu5gBsmAppIndex);
    if (app == nullptr || !app->hasGate(SIMU5G_BRIDGE_GATE)) {
        return false;
    }

    const std::string destAddress = simu5gVehicleModuleName + "[" + std::to_string(receiverIndex) + "]";
    const int senderId = metric.value("sender_id", 0);
    const int receiverId = metric.value("receiver_id", 0);
    const int payloadBytes = metric.value("payload_bytes", 0);
    const std::string content = bsmContent(message);
    std::string messageName = bsmMessageName(message);
    if (messageName.empty()) {
        messageName = "BasicSafetyMessage";
    }
    const std::string messageStandard = bsmMessageStandard(message);
    const json payloadJson = bsmPayloadJson(message);
    const std::string payloadJsonString = payloadJson.dump();

    auto* tx = new omnetpp::cMessage("metsrBsmUuTx", KIND_SIMU5G_BSM_REQUEST);
    tx->addPar("message_id") = transportMessageId.c_str();
    tx->addPar("sender_id") = senderId;
    tx->addPar("receiver_id") = receiverId;
    tx->addPar("sender_index") = senderIndex;
    tx->addPar("receiver_index") = receiverIndex;
    tx->addPar("dest_address") = destAddress.c_str();
    tx->addPar("dest_port") = simu5gDestPort;
    tx->addPar("local_port") = simu5gLocalPort;
    tx->addPar("payload_bytes") = payloadBytes;
    tx->addPar("payload_bit_length") = bsmPayloadBitLength(message);
    tx->addPar("payload_encoding") = bsmPayloadEncoding(message).c_str();
    tx->addPar("tx_time_s") = generationTimeS;
    tx->addPar("message_name") = messageName.c_str();
    tx->addPar("message_standard") = messageStandard.c_str();
    tx->addPar("message_json") = payloadJsonString.c_str();
    tx->addPar("content") = content.c_str();
    const std::string wirePayloadB64 = bsmWireDataB64(message);
    if (!wirePayloadB64.empty()) {
        tx->addPar("wire_payload_b64") = wirePayloadB64.c_str();
    }

    const auto inserted = pendingSimu5gDeliveries.emplace(
        transportMessageId,
        Simu5gPendingDelivery {
        work,
        message,
        metric,
        generationTimeS,
        });
    if (!inserted.second) {
        delete tx;
        return false;
    }
    work->outstandingDeliveries += 1;
    sendDirect(tx, app, SIMU5G_BRIDGE_GATE);
    return true;
}

bool MetsrVeinsBridge::runSimu5gCellularUuBackend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
    if (!messages.empty() && simu5gVehicleModule(0) == nullptr) {
        json errorEvent = {
            {"tick", work->tick},
            {"status", "error"},
            {"message", "simu5g_cellular_uu requires a Simu5G network with UE submodules"},
            {"vehicle_module_name", simu5gVehicleModuleName},
            {"requested_backend", bridgeBackend},
            {"requested_backend_implementation", backendImplementation},
        };
        addBridgeMetadata(errorEvent, true);
        work->attackEvents.push_back(errorEvent);
        completeSyncWork(work, "error", errorEvent["message"].get<std::string>());
        return false;
    }

    std::map<int, json> vehicleById;
    std::map<int, int> vehicleIndexById;
    std::set<int> capacityRejectedVehicleIds;
    for (const auto& vehicle : vehicles) {
        const int vehicleId = entityId(vehicle, "vehicle_id", entityId(vehicle, "id", 0));
        if (vehicleId == 0) {
            continue;
        }
        const int vehicleIndex = ensureSimu5gVehicleIndex(vehicleId);
        if (vehicleIndex < 0) {
            capacityRejectedVehicleIds.insert(vehicleId);
            EV_WARN << "No free Simu5G UE slot exists for METS-R vehicle_id="
                    << vehicleId << std::endl;
            continue;
        }
        vehicleById[vehicleId] = vehicle;
        vehicleIndexById[vehicleId] = vehicleIndex;
        if (!updateSimu5gMobility(vehicleIndex, vehicleId, vehicle)) {
            EV_WARN << "Could not update Simu5G mobility for vehicle_id=" << vehicleId
                    << " index=" << vehicleIndex << "\n";
        }
    }

    const double generationTimeS = omnetpp::simTime().dbl();
    auto makeUuMetric = [&](const json& message,
                            int senderId,
                            int receiverId,
                            const std::string& sourceMessageId,
                            bool broadcast) {
        json metric = {
            {"tick", work->tick},
            {"source_message_id", sourceMessageId},
            {"message_id", broadcast
                ? linkMessageId(sourceMessageId, receiverId)
                : sourceMessageId},
            {"application_tx_time_s", bsmTxTimeS(message, generationTimeS)},
            {"bridge_tx_time_s", generationTimeS},
            {"tx_time_s", generationTimeS},
            {"sender_id", senderId},
            {"receiver_id", receiverId > 0 ? json(receiverId) : json(nullptr)},
            {"message_name", bsmMessageName(message)},
            {"message_standard", bsmMessageStandard(message)},
            {"message_count", bsmMessageCount(message)},
            {"payload_bytes", bsmPayloadBytes(message)},
            {"payload_bit_length", bsmPayloadBitLength(message)},
            {"payload_encoding", bsmPayloadEncoding(message)},
            {"wire_payload_present", !bsmWireDataB64(message).empty()},
            {"radio_mode", bsmRadioMode(message, radioAccess)},
            {"attacked", bsmAttackValue(message, "attacked", "attacked", json(false))},
            {"attack_id", bsmAttackString(message, "attack_id", "attackId")},
            {"attack_type", bsmAttackString(message, "attack_type", "attackType")},
            {"backend_time_source", "simu5g_udp_receive_event"},
            {"simu5g_delivery_path", "5g_nr_uu_unicast"},
            {"broadcast_delivery", broadcast ? "expanded_to_uu_unicast" : "unicast"},
            {"physical_transmission_count", 1},
            {"delivered", false},
            {"drop_reason", "pending_simu5g_delivery"},
        };
        if (vehicleById.count(senderId) != 0 && vehicleById.count(receiverId) != 0) {
            metric["distance_m"] = distanceM(
                vehicleById.at(senderId),
                vehicleById.at(receiverId));
        }
        else {
            metric["distance_m"] = nullptr;
        }
        addBridgeMetadata(metric);
        return metric;
    };

    std::size_t transmissionSequence = 0;
    for (const auto& message : messages) {
        const int senderId = bsmSenderId(message);
        const int explicitReceiver = bsmReceiverId(message);
        const auto expectedIt = message.find("expected_receiver_ids");
        const bool hasExpectedReceiverList = expectedIt != message.end();
        const std::string sourceMessageId = messageIdValue(
            message, work->tick, senderId, explicitReceiver);
        if (hasExpectedReceiverList && !expectedIt->is_array()) {
            json metric = makeUuMetric(
                message, senderId, explicitReceiver, sourceMessageId, true);
            metric["physical_transmission_count"] = 0;
            metric["drop_reason"] = "invalid_expected_receiver_ids";
            work->metrics.push_back(std::move(metric));
            continue;
        }

        std::set<int> requestedReceivers;
        for (const int receiverId : explicitExpectedReceivers(message)) {
            requestedReceivers.insert(receiverId);
        }
        if (!hasExpectedReceiverList && explicitReceiver > 0) {
            requestedReceivers.insert(explicitReceiver);
        }
        if (!hasExpectedReceiverList && explicitReceiver <= 0) {
            for (const auto& vehicle : vehicleById) {
                if (vehicle.first != senderId) {
                    requestedReceivers.insert(vehicle.first);
                }
            }
        }
        if (hasExpectedReceiverList && requestedReceivers.empty()) {
            json metric = makeUuMetric(
                message, senderId, explicitReceiver, sourceMessageId, true);
            metric["physical_transmission_count"] = 0;
            metric["drop_reason"] = "no_expected_receivers";
            work->metrics.push_back(std::move(metric));
            continue;
        }

        const std::string wirePayloadError =
            bsmWirePayloadValidationError(message);
        for (const int receiverId : requestedReceivers) {
            const bool broadcast = hasExpectedReceiverList || explicitReceiver <= 0;
            json metric = makeUuMetric(
                message, senderId, receiverId, sourceMessageId, broadcast);
            std::string dropReason;
            if (!wirePayloadError.empty()) {
                dropReason = wirePayloadError;
            }
            else if (receiverId == senderId) {
                dropReason = "receiver_is_sender";
            }
            else if (vehicleIndexById.count(senderId) == 0 ||
                     vehicleIndexById.count(receiverId) == 0) {
                dropReason =
                    capacityRejectedVehicleIds.count(senderId) != 0 ||
                    capacityRejectedVehicleIds.count(receiverId) != 0
                    ? "simu5g_vehicle_capacity_exhausted"
                    : "unknown_sender_or_receiver_vehicle";
            }
            if (!dropReason.empty()) {
                metric["physical_transmission_count"] = 0;
                metric["drop_reason"] = dropReason;
                work->metrics.push_back(std::move(metric));
                continue;
            }

            const std::string transportMessageId =
                "uu:" + std::to_string(work->requestId) + ":" +
                std::to_string(work->tick) + ":" +
                std::to_string(senderId) + ":" +
                std::to_string(transmissionSequence++);
            metric["transmission_id"] = transportMessageId;
            if (!injectSimu5gBsm(
                    work,
                    message,
                    metric,
                    transportMessageId,
                    vehicleIndexById.at(senderId),
                    vehicleIndexById.at(receiverId),
                    generationTimeS)) {
                metric["physical_transmission_count"] = 0;
                metric["drop_reason"] = "simu5g_sender_app_missing";
                work->metrics.push_back(std::move(metric));
            }
        }
    }

    if (work->outstandingDeliveries > 0) {
        auto* timeout = new omnetpp::cMessage("simu5gSyncTimeout", KIND_SIMU5G_SYNC_TIMEOUT);
        timeout->setContextPointer(new SyncWorkHolder {work});
        work->timeoutEvent = timeout;
        scheduleAt(
            omnetpp::simTime() + boundedReceiveTimeoutS(work, simu5gReceiveTimeoutMs),
            timeout);
    }

    return true;
}

bool MetsrVeinsBridge::injectSimu5gPc5Transmission(
    Simu5gPc5PendingTransmission& pending,
    int senderIndex)
{
    omnetpp::cModule* sender = simu5gVehicleModule(senderIndex);
    if (sender == nullptr) {
        return false;
    }
    omnetpp::cModule* app =
        sender->getSubmodule("app", simu5gBsmAppIndex);
    if (app == nullptr || !app->hasGate(SIMU5G_BRIDGE_GATE)) {
        return false;
    }

    std::set<int> receiverIds;
    for (const auto& receiver : pending.receiverIdByIndex) {
        receiverIds.insert(receiver.second);
    }
    const json& message = pending.message;
    auto* tx = new omnetpp::cMessage(
        "metsrBsmPc5MulticastTx",
        KIND_SIMU5G_BSM_REQUEST);
    tx->addPar("message_id") = pending.transmissionId.c_str();
    tx->addPar("sender_id") = bsmSenderId(message);
    tx->addPar("sender_index") = senderIndex;
    tx->addPar("receiver_id") = -1;
    tx->addPar("dest_port") = simu5gDestPort;
    tx->addPar("local_port") = simu5gLocalPort;
    tx->addPar("payload_bytes") = bsmPayloadBytes(message);
    tx->addPar("payload_bit_length") =
        bsmPayloadBitLength(message);
    tx->addPar("payload_encoding") =
        bsmPayloadEncoding(message).c_str();
    tx->addPar("tx_time_s") =
        bsmTxTimeS(message, pending.generationTimeS);
    tx->addPar("message_name") = bsmMessageName(message).c_str();
    tx->addPar("message_standard") =
        bsmMessageStandard(message).c_str();
    tx->addPar("content") = bsmContent(message).c_str();
    tx->addPar("expected_receiver_ids") =
        joinReceiverIds(receiverIds).c_str();
    if (!pending.wirePayloadB64.empty()) {
        tx->addPar("wire_payload_b64") =
            pending.wirePayloadB64.c_str();
    }
    sendDirect(tx, app, SIMU5G_BRIDGE_GATE);
    return true;
}

bool MetsrVeinsBridge::runSimu5gPc5Backend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
    if (!messages.empty() && simu5gVehicleModule(0) == nullptr) {
        json errorEvent = {
            {"tick", work->tick},
            {"status", "error"},
            {"message", "simu5g_lte_d2d_pc5 requires D2D-capable Simu5G UE submodules"},
            {"vehicle_module_name", simu5gVehicleModuleName},
            {"requested_backend", bridgeBackend},
            {"requested_backend_implementation", backendImplementation},
        };
        addBridgeMetadata(errorEvent, true);
        work->attackEvents.push_back(errorEvent);
        completeSyncWork(
            work,
            "error",
            errorEvent["message"].get<std::string>());
        return false;
    }

    std::map<int, json> vehicleById;
    std::map<int, int> vehicleIndexById;
    std::set<int> capacityRejectedVehicleIds;
    for (const auto& vehicle : vehicles) {
        const int vehicleId =
            entityId(vehicle, "vehicle_id", entityId(vehicle, "id", 0));
        if (vehicleId == 0) {
            continue;
        }
        const int vehicleIndex = ensureSimu5gVehicleIndex(vehicleId);
        if (vehicleIndex < 0) {
            capacityRejectedVehicleIds.insert(vehicleId);
            EV_WARN << "No free Simu5G UE slot exists for METS-R vehicle_id="
                    << vehicleId
                    << std::endl;
            continue;
        }
        vehicleById[vehicleId] = vehicle;
        vehicleIndexById[vehicleId] = vehicleIndex;
        if (!updateSimu5gMobility(vehicleIndex, vehicleId, vehicle)) {
            EV_WARN << "Could not update Simu5G PC5 mobility for vehicle_id="
                    << vehicleId << " index=" << vehicleIndex
                    << std::endl;
        }
    }

    auto addImmediateDrop = [&](const json& message,
                                int senderId,
                                int receiverId,
                                const std::string& reason) {
        json metric = {
            {"tick", work->tick},
            {"message_id", messageIdValue(
                message, work->tick, senderId, receiverId)},
            {"transmission_id", nullptr},
            {"tx_time_s", bsmTxTimeS(
                message, omnetpp::simTime().dbl())},
            {"sender_id", senderId},
            {"receiver_id", receiverId > 0
                ? json(receiverId)
                : json(nullptr)},
            {"message_name", bsmMessageName(message)},
            {"message_standard", bsmMessageStandard(message)},
            {"message_count", bsmMessageCount(message)},
            {"payload_bytes", bsmPayloadBytes(message)},
            {"payload_bit_length", bsmPayloadBitLength(message)},
            {"payload_encoding", bsmPayloadEncoding(message)},
            {"radio_mode", "cv2x_pc5"},
            {"backend_time_source", "simu5g_d2d_multicast_receive_event"},
            {"simu5g_delivery_path", "simu5g_lte_d2d_pc5_multicast"},
            {"pc5_resource_control", "network_scheduled"},
            {"physical_transmission_count", 0},
            {"delivered", false},
            {"drop_reason", reason},
            {"rx_time_s", nullptr},
            {"latency_ms", nullptr},
        };
        addBridgeMetadata(metric);
        work->metrics.push_back(std::move(metric));
    };

    const double generationTimeS = omnetpp::simTime().dbl();
    std::vector<Simu5gPc5PendingTransmission> transmissions;
    std::map<std::string, std::size_t> groupIndex;
    std::size_t messageSequence = 0;
    for (const auto& message : messages) {
        const int senderId = bsmSenderId(message);
        const int explicitReceiver = bsmReceiverId(message);
        const auto expectedIt = message.find("expected_receiver_ids");
        const bool hasExpectedReceiverList =
            expectedIt != message.end();
        if (hasExpectedReceiverList && !expectedIt->is_array()) {
            addImmediateDrop(
                message,
                senderId,
                explicitReceiver,
                "invalid_expected_receiver_ids");
            messageSequence += 1;
            continue;
        }

        std::set<int> requestedReceivers;
        for (const int receiverId : explicitExpectedReceivers(message)) {
            requestedReceivers.insert(receiverId);
        }
        if (!hasExpectedReceiverList && explicitReceiver > 0) {
            requestedReceivers.insert(explicitReceiver);
        }
        if (!hasExpectedReceiverList && explicitReceiver <= 0) {
            for (const auto& vehicle : vehicleById) {
                if (vehicle.first != senderId) {
                    requestedReceivers.insert(vehicle.first);
                }
            }
        }
        if (hasExpectedReceiverList && requestedReceivers.empty()) {
            addImmediateDrop(
                message,
                senderId,
                explicitReceiver,
                "no_expected_receivers");
            messageSequence += 1;
            continue;
        }

        if (senderId <= 0 ||
            vehicleIndexById.find(senderId) == vehicleIndexById.end()) {
            const std::string reason =
                capacityRejectedVehicleIds.count(senderId) != 0
                    ? "simu5g_vehicle_capacity_exhausted"
                    : "unknown_sender_vehicle";
            if (requestedReceivers.empty()) {
                addImmediateDrop(
                    message,
                    senderId,
                    explicitReceiver,
                    reason);
            }
            else {
                for (const int receiverId : requestedReceivers) {
                    addImmediateDrop(
                        message,
                        senderId,
                        receiverId,
                        reason);
                }
            }
            messageSequence += 1;
            continue;
        }

        std::set<int> validReceivers;
        for (const int receiverId : requestedReceivers) {
            if (receiverId == senderId) {
                addImmediateDrop(
                    message,
                    senderId,
                    receiverId,
                    "receiver_is_sender");
                continue;
            }
            if (vehicleIndexById.find(receiverId) ==
                vehicleIndexById.end()) {
                addImmediateDrop(
                    message,
                    senderId,
                    receiverId,
                    capacityRejectedVehicleIds.count(receiverId) != 0
                        ? "simu5g_vehicle_capacity_exhausted"
                        : "unknown_receiver_vehicle");
                continue;
            }
            validReceivers.insert(receiverId);
        }
        if (validReceivers.empty()) {
            messageSequence += 1;
            continue;
        }

        const bool legacyPair =
            explicitReceiver > 0 &&
            !hasExpectedReceiverList &&
            bsmMessageName(message) == "BasicSafetyMessage";
        const std::string wirePayloadB64 = bsmWireDataB64(message);
        const std::string wirePayloadError =
            bsmWirePayloadValidationError(message);
        if (!wirePayloadError.empty()) {
            for (const int receiverId : validReceivers) {
                addImmediateDrop(
                    message,
                    senderId,
                    receiverId,
                    wirePayloadError);
            }
            messageSequence += 1;
            continue;
        }
        std::ostringstream signature;
        signature << senderId << "|" << bsmTxTimeS(message, -1.0)
                  << "|" << bsmMessageName(message)
                  << "|" << bsmMessageStandard(message)
                  << "|" << bsmMessageCount(message)
                  << "|" << bsmPayloadBytes(message)
                  << "|" << bsmPayloadBitLength(message)
                  << "|" << bsmPayloadEncoding(message)
                  << "|" << wirePayloadB64
                  << "|" << bsmContent(message)
                  << "|" << bsmAttackString(message, "attack_id", "attackId")
                  << "|" << bsmAttackString(message, "attack_type", "attackType")
                  << "|" << stringValue(message, "x")
                  << "|" << stringValue(message, "y")
                  << "|" << stringValue(message, "z")
                  << "|" << stringValue(message, "speed_mps")
                  << "|" << stringValue(message, "heading_deg");

        std::string explicitGroup =
            stringValue(message, "broadcast_id");
        if (explicitGroup.empty()) {
            explicitGroup = stringValue(message, "transmission_id");
        }
        const bool groupable = legacyPair || !explicitGroup.empty();
        const std::string groupingKey = groupable
            ? (explicitGroup.empty()
                    ? "derived|"
                    : "explicit|" + explicitGroup + "|") +
                signature.str()
            : "unique|" + std::to_string(work->requestId) + "|" +
                std::to_string(messageSequence);

        std::size_t index = transmissions.size();
        auto existing = groupIndex.find(groupingKey);
        bool overlapsDerivedGroup = false;
        if (legacyPair && explicitGroup.empty() &&
            existing != groupIndex.end()) {
            for (const int receiverId : validReceivers) {
                const int receiverIndex = vehicleIndexById.at(receiverId);
                if (transmissions[existing->second]
                        .expectedReceiverIndexes.count(receiverIndex) != 0) {
                    overlapsDerivedGroup = true;
                    break;
                }
            }
        }
        if (groupable && existing != groupIndex.end() &&
            !overlapsDerivedGroup) {
            index = existing->second;
            transmissions[index].metric["legacy_pairwise_consolidated"] =
                transmissions[index].metric.value(
                    "legacy_pairwise_consolidated", false) || legacyPair;
        }
        else {
            Simu5gPc5PendingTransmission pending;
            pending.work = work;
            pending.message = message;
            pending.generationTimeS = generationTimeS;
            pending.transmissionId =
                "pc5:" + std::to_string(work->requestId) + ":" +
                std::to_string(work->tick) + ":" +
                std::to_string(senderId) + ":" +
                std::to_string(transmissions.size());
            pending.wirePayloadB64 = wirePayloadB64;
            pending.metric = {
                {"tick", work->tick},
                {"transmission_id", pending.transmissionId},
                {"source_message_id", messageIdValue(
                    message, work->tick, senderId, explicitReceiver)},
                {"application_tx_time_s", bsmTxTimeS(message, generationTimeS)},
                {"bridge_tx_time_s", generationTimeS},
                {"tx_time_s", generationTimeS},
                {"generation_time_s", generationTimeS},
                {"sender_id", senderId},
                {"message_name", bsmMessageName(message)},
                {"message_standard", bsmMessageStandard(message)},
                {"message_count", bsmMessageCount(message)},
                {"payload_bytes", bsmPayloadBytes(message)},
                {"payload_bit_length", bsmPayloadBitLength(message)},
                {"payload_encoding", bsmPayloadEncoding(message)},
                {"wire_payload_present", !wirePayloadB64.empty()},
                {"radio_mode", "cv2x_pc5"},
                {"backend_time_source", "simu5g_d2d_multicast_receive_event"},
                {"simu5g_delivery_path", "simu5g_lte_d2d_pc5_multicast"},
                {"pc5_resource_control", "network_scheduled"},
                {"physical_transmission_count", 1},
                {"legacy_pairwise_consolidated", false},
                {"attacked", bsmAttackValue(
                    message, "attacked", "attacked", json(false))},
                {"attack_id", bsmAttackString(
                    message, "attack_id", "attackId")},
                {"attack_type", bsmAttackString(
                    message, "attack_type", "attackType")},
            };
            addBridgeMetadata(pending.metric);
            transmissions.push_back(std::move(pending));
            index = transmissions.size() - 1;
            if (groupable) {
                groupIndex[groupingKey] = index;
            }
        }

        for (const int receiverId : validReceivers) {
            const int receiverIndex = vehicleIndexById.at(receiverId);
            json receiverMessage = message;
            receiverMessage["_bridge_distance_m"] =
                distanceM(
                    vehicleById.at(senderId),
                    vehicleById.at(receiverId));
            receiverMessage["_pc5_legacy_pair"] = legacyPair;
            transmissions[index].expectedReceiverIndexes.insert(
                receiverIndex);
            transmissions[index].receiverIdByIndex[receiverIndex] =
                receiverId;
            transmissions[index].messageByReceiverIndex[receiverIndex] =
                std::move(receiverMessage);
        }
        messageSequence += 1;
    }

    for (auto& pending : transmissions) {
        if (pending.expectedReceiverIndexes.empty()) {
            continue;
        }
        const int senderId = bsmSenderId(pending.message);
        const int senderIndex = vehicleIndexById.at(senderId);
        work->outstandingDeliveries += static_cast<int>(
            pending.expectedReceiverIndexes.size());
        const std::string transmissionId = pending.transmissionId;
        pendingSimu5gPc5Transmissions[transmissionId] =
            std::move(pending);
        auto& stored =
            pendingSimu5gPc5Transmissions.at(transmissionId);
        if (!injectSimu5gPc5Transmission(stored, senderIndex)) {
            const std::vector<int> receiverIndexes(
                stored.expectedReceiverIndexes.begin(),
                stored.expectedReceiverIndexes.end());
            for (const int receiverIndex : receiverIndexes) {
                markSimu5gPc5Drop(
                    stored,
                    receiverIndex,
                    "simu5g_pc5_sender_app_missing");
            }
            pendingSimu5gPc5Transmissions.erase(transmissionId);
        }
    }

    if (work->outstandingDeliveries > 0) {
        auto* timeout = new omnetpp::cMessage(
            "simu5gPc5SyncTimeout",
            KIND_SIMU5G_SYNC_TIMEOUT);
        timeout->setContextPointer(new SyncWorkHolder {work});
        work->timeoutEvent = timeout;
        scheduleAt(
            omnetpp::simTime() +
                boundedReceiveTimeoutS(work, simu5gReceiveTimeoutMs),
            timeout);
    }
    return true;
}

void MetsrVeinsBridge::markSimu5gPc5Drop(
    Simu5gPc5PendingTransmission& pending,
    int receiverIndex,
    const std::string& dropReason)
{
    if (pending.completedReceiverIndexes.find(receiverIndex) !=
        pending.completedReceiverIndexes.end()) {
        return;
    }
    auto receiverIdIt =
        pending.receiverIdByIndex.find(receiverIndex);
    auto receiverMessageIt =
        pending.messageByReceiverIndex.find(receiverIndex);
    if (receiverIdIt == pending.receiverIdByIndex.end() ||
        receiverMessageIt ==
            pending.messageByReceiverIndex.end()) {
        return;
    }

    const int receiverId = receiverIdIt->second;
    const json& receiverMessage = receiverMessageIt->second;
    const bool legacyPair =
        receiverMessage.value("_pc5_legacy_pair", false);
    json metric = pending.metric;
    metric["message_id"] = legacyPair
        ? messageIdValue(
              receiverMessage,
              pending.work->tick,
              bsmSenderId(receiverMessage),
              receiverId)
        : linkMessageId(pending.transmissionId, receiverId);
    if (legacyPair) {
        metric["source_message_id"] = metric["message_id"];
    }
    metric["receiver_id"] = receiverId;
    metric["receiver_index"] = receiverIndex;
    metric["distance_m"] =
        receiverMessage.value("_bridge_distance_m", 0.0);
    metric["delivered"] = false;
    metric["drop_reason"] = dropReason;
    metric["rx_time_s"] = nullptr;
    metric["latency_ms"] = nullptr;
    metric["wire_payload_verified"] = nullptr;
    addBridgeMetadata(metric);
    pending.work->metrics.push_back(std::move(metric));
    pending.completedReceiverIndexes.insert(receiverIndex);
    pending.work->outstandingDeliveries = std::max(
        0,
        pending.work->outstandingDeliveries - 1);
}

void MetsrVeinsBridge::markSimu5gDrop(
    const Simu5gPendingDelivery& delivery,
    const std::string& dropReason)
{
    json metric = delivery.metric;
    metric["delivered"] = false;
    metric["drop_reason"] = dropReason;
    metric["rx_time_s"] = nullptr;
    metric["latency_ms"] = nullptr;
    addBridgeMetadata(metric);
    delivery.work->metrics.push_back(metric);
    delivery.work->outstandingDeliveries = std::max(0, delivery.work->outstandingDeliveries - 1);
}

void MetsrVeinsBridge::handleSimu5gReport(omnetpp::cMessage* message)
{
    std::unique_ptr<omnetpp::cMessage> cleanup(message);
    const std::string messageId = message->hasPar("message_id")
        ? message->par("message_id").stringValue()
        : "";

    auto pc5It = pendingSimu5gPc5Transmissions.find(messageId);
    if (pc5It != pendingSimu5gPc5Transmissions.end()) {
        Simu5gPc5PendingTransmission& pending = pc5It->second;
        const int receiverIndex = message->hasPar("receiver_index")
            ? static_cast<int>(
                  message->par("receiver_index").longValue())
            : -1;
        if (pending.expectedReceiverIndexes.find(receiverIndex) ==
                pending.expectedReceiverIndexes.end() ||
            pending.completedReceiverIndexes.find(receiverIndex) !=
                pending.completedReceiverIndexes.end()) {
            return;
        }
        const int receiverId =
            pending.receiverIdByIndex.at(receiverIndex);
        json receiverMessage =
            pending.messageByReceiverIndex.at(receiverIndex);
        const bool legacyPair =
            receiverMessage.value("_pc5_legacy_pair", false);
        const std::string receivedWirePayloadB64 =
            message->hasPar("wire_payload_b64")
            ? message->par("wire_payload_b64").stringValue()
            : "";
        const bool expectsWirePayload =
            !pending.wirePayloadB64.empty();
        const bool wirePayloadVerified =
            !expectsWirePayload ||
            receivedWirePayloadB64 == pending.wirePayloadB64;
        if (!wirePayloadVerified) {
            markSimu5gPc5Drop(
                pending,
                receiverIndex,
                "simu5g_pc5_wire_payload_mismatch");
            json& metric = pending.work->metrics.back();
            metric["wire_payload_present"] =
                !receivedWirePayloadB64.empty();
            metric["wire_payload_verified"] = false;
            metric["reported_payload_bytes"] =
                message->hasPar("payload_bytes")
                ? message->par("payload_bytes").longValue()
                : 0;
            const auto work = pending.work;
            if (pending.completedReceiverIndexes.size() ==
                pending.expectedReceiverIndexes.size()) {
                pendingSimu5gPc5Transmissions.erase(pc5It);
            }
            if (work->outstandingDeliveries == 0) {
                completeSyncWork(work);
            }
            return;
        }

        const double rxTimeS = omnetpp::simTime().dbl();
        json metric = pending.metric;
        metric["message_id"] = legacyPair
            ? messageIdValue(
                  receiverMessage,
                  pending.work->tick,
                  bsmSenderId(receiverMessage),
                  receiverId)
            : linkMessageId(
                  pending.transmissionId,
                  receiverId);
        if (legacyPair) {
            metric["source_message_id"] = metric["message_id"];
        }
        metric["receiver_id"] = receiverId;
        metric["receiver_index"] = receiverIndex;
        metric["distance_m"] =
            receiverMessage.value("_bridge_distance_m", 0.0);
        metric["delivered"] = true;
        metric["drop_reason"] = "";
        metric["rx_time_s"] = rxTimeS;
        metric["latency_ms"] =
            (rxTimeS - pending.generationTimeS) * 1000.0;
        metric["simu5g_sender_module"] =
            message->hasPar("sender_module")
            ? message->par("sender_module").stringValue()
            : "";
        metric["simu5g_receiver_module"] =
            message->hasPar("receiver_module")
            ? message->par("receiver_module").stringValue()
            : "";
        metric["simu5g_receiver_app"] =
            message->hasPar("receiver_app")
            ? message->par("receiver_app").stringValue()
            : "";
        metric["simu5g_packet_name"] =
            message->hasPar("packet_name")
            ? message->par("packet_name").stringValue()
            : "";
        metric["simu5g_delivery_path"] =
            message->hasPar("delivery_path")
            ? message->par("delivery_path").stringValue()
            : "simu5g_lte_d2d_pc5_multicast";
        metric["pc5_resource_control"] =
            message->hasPar("pc5_resource_control")
            ? message->par("pc5_resource_control").stringValue()
            : "network_scheduled";
        metric["payload_bytes"] =
            message->hasPar("payload_bytes")
            ? message->par("payload_bytes").longValue()
            : metric.value("payload_bytes", 0);
        metric["wire_payload_present"] =
            message->hasPar("wire_payload_present")
            ? message->par("wire_payload_present").boolValue()
            : !receivedWirePayloadB64.empty();
        metric["wire_payload_transport_encoding"] =
            message->hasPar("wire_payload_encoding")
            ? message->par("wire_payload_encoding").stringValue()
            : "";
        metric["wire_payload_verified"] =
            expectsWirePayload ? json(true) : json(nullptr);
        addBridgeMetadata(metric);
        pending.work->metrics.push_back(metric);

        receiverMessage.erase("_bridge_distance_m");
        receiverMessage.erase("_pc5_legacy_pair");
        receiverMessage["source_message_id"] =
            metric["source_message_id"];
        receiverMessage["message_id"] = metric["message_id"];
        receiverMessage["transmission_id"] =
            pending.transmissionId;
        receiverMessage["sender_id"] = metric["sender_id"];
        receiverMessage["receiver_id"] = receiverId;
        receiverMessage["receiver_index"] = receiverIndex;
        receiverMessage["distance_m"] = metric["distance_m"];
        receiverMessage["application_tx_time_s"] =
            metric["application_tx_time_s"];
        receiverMessage["bridge_tx_time_s"] =
            metric["bridge_tx_time_s"];
        receiverMessage["tx_time_s"] = metric["tx_time_s"];
        receiverMessage["rx_time_s"] = metric["rx_time_s"];
        receiverMessage["latency_ms"] = metric["latency_ms"];
        receiverMessage["payload_bytes"] =
            metric["payload_bytes"];
        receiverMessage["wire_payload_verified"] =
            metric["wire_payload_verified"];
        receiverMessage["simu5g_delivery_path"] =
            metric["simu5g_delivery_path"];
        receiverMessage["pc5_resource_control"] =
            metric["pc5_resource_control"];
        addBridgeMetadata(receiverMessage);
        pending.work->received.push_back(
            std::move(receiverMessage));

        pending.completedReceiverIndexes.insert(receiverIndex);
        pending.work->outstandingDeliveries = std::max(
            0,
            pending.work->outstandingDeliveries - 1);
        const auto work = pending.work;
        if (pending.completedReceiverIndexes.size() ==
            pending.expectedReceiverIndexes.size()) {
            pendingSimu5gPc5Transmissions.erase(pc5It);
        }
        if (work->outstandingDeliveries == 0) {
            completeSyncWork(work);
        }
        return;
    }

    auto it = pendingSimu5gDeliveries.find(messageId);
    if (it == pendingSimu5gDeliveries.end()) {
        EV_WARN << "Ignoring Simu5G receive report for unknown message_id=" << messageId << "\n";
        return;
    }

    Simu5gPendingDelivery delivery = it->second;
    pendingSimu5gDeliveries.erase(it);

    const double rxTimeS = omnetpp::simTime().dbl();
    const std::string expectedWirePayloadB64 =
        bsmWireDataB64(delivery.message);
    const std::string receivedWirePayloadB64 =
        message->hasPar("wire_payload_b64")
        ? message->par("wire_payload_b64").stringValue()
        : "";
    const bool expectsWirePayload = !expectedWirePayloadB64.empty();
    const bool wirePayloadVerified =
        !expectsWirePayload ||
        receivedWirePayloadB64 == expectedWirePayloadB64;
    json metric = delivery.metric;
    metric["delivered"] = wirePayloadVerified;
    metric["drop_reason"] = wirePayloadVerified
        ? ""
        : "simu5g_uu_wire_payload_mismatch";
    metric["rx_time_s"] = rxTimeS;
    metric["latency_ms"] = (rxTimeS - delivery.generationTimeS) * 1000.0;
    metric["reported_payload_bytes"] = message->hasPar("payload_bytes")
        ? message->par("payload_bytes").longValue()
        : 0;
    metric["wire_payload_present"] = message->hasPar("wire_payload_present")
        ? message->par("wire_payload_present").boolValue()
        : !receivedWirePayloadB64.empty();
    metric["wire_payload_transport_encoding"] =
        message->hasPar("wire_payload_encoding")
        ? message->par("wire_payload_encoding").stringValue()
        : "";
    metric["wire_payload_verified"] = expectsWirePayload
        ? json(wirePayloadVerified)
        : json(nullptr);
    metric["simu5g_sender_module"] = message->hasPar("sender_module")
        ? message->par("sender_module").stringValue()
        : "";
    metric["simu5g_receiver_module"] = message->hasPar("receiver_module")
        ? message->par("receiver_module").stringValue()
        : "";
    metric["simu5g_receiver_app"] = message->hasPar("receiver_app")
        ? message->par("receiver_app").stringValue()
        : "";
    metric["simu5g_packet_name"] = message->hasPar("packet_name")
        ? message->par("packet_name").stringValue()
        : "";
    addBridgeMetadata(metric);
    delivery.work->metrics.push_back(metric);

    if (wirePayloadVerified) {
        json deliveredMessage = delivery.message;
        deliveredMessage["source_message_id"] = metric["source_message_id"];
        deliveredMessage["message_id"] = metric["message_id"];
        deliveredMessage["transmission_id"] = metric["transmission_id"];
        deliveredMessage["receiver_id"] = metric["receiver_id"];
        deliveredMessage["application_tx_time_s"] =
            metric["application_tx_time_s"];
        deliveredMessage["bridge_tx_time_s"] = metric["bridge_tx_time_s"];
        deliveredMessage["tx_time_s"] = metric["tx_time_s"];
        deliveredMessage["rx_time_s"] = metric["rx_time_s"];
        deliveredMessage["latency_ms"] = metric["latency_ms"];
        deliveredMessage["distance_m"] = metric["distance_m"];
        deliveredMessage["radio_mode"] = metric["radio_mode"];
        deliveredMessage["attacked"] = metric["attacked"];
        deliveredMessage["attack_id"] = metric["attack_id"];
        deliveredMessage["attack_type"] = metric["attack_type"];
        deliveredMessage["wire_payload_verified"] =
            metric["wire_payload_verified"];
        deliveredMessage["simu5g_delivery_path"] =
            metric["simu5g_delivery_path"];
        addBridgeMetadata(deliveredMessage);
        delivery.work->received.push_back(std::move(deliveredMessage));
    }

    delivery.work->outstandingDeliveries = std::max(0, delivery.work->outstandingDeliveries - 1);
    if (delivery.work->outstandingDeliveries == 0) {
        completeSyncWork(delivery.work);
    }
}

void MetsrVeinsBridge::handleSimu5gTimeout(omnetpp::cMessage* message)
{
    std::unique_ptr<omnetpp::cMessage> cleanup(message);
    auto* holder = static_cast<SyncWorkHolder*>(message->getContextPointer());
    std::shared_ptr<SyncWork> work = holder != nullptr ? holder->work : nullptr;
    delete holder;
    if (!work) {
        return;
    }
    work->timeoutEvent = nullptr;

    for (auto it = pendingSimu5gDeliveries.begin(); it != pendingSimu5gDeliveries.end(); ) {
        if (it->second.work == work) {
            markSimu5gDrop(it->second, "simu5g_receive_timeout");
            it = pendingSimu5gDeliveries.erase(it);
        }
        else {
            ++it;
        }
    }

    for (auto it = pendingSimu5gPc5Transmissions.begin();
         it != pendingSimu5gPc5Transmissions.end();) {
        if (it->second.work != work) {
            ++it;
            continue;
        }
        std::vector<int> missingReceiverIndexes;
        for (const int receiverIndex :
             it->second.expectedReceiverIndexes) {
            if (it->second.completedReceiverIndexes.find(
                    receiverIndex) ==
                it->second.completedReceiverIndexes.end()) {
                missingReceiverIndexes.push_back(receiverIndex);
            }
        }
        for (const int receiverIndex : missingReceiverIndexes) {
            markSimu5gPc5Drop(
                it->second,
                receiverIndex,
                "simu5g_pc5_receive_timeout");
        }
        it = pendingSimu5gPc5Transmissions.erase(it);
    }

    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
}

void MetsrVeinsBridge::expireSyncWorkDeliveries(
    const std::shared_ptr<SyncWork>& work)
{
    for (auto it = pendingSimu5gDeliveries.begin();
         it != pendingSimu5gDeliveries.end();) {
        if (it->second.work == work) {
            markSimu5gDrop(it->second, "sync_tick_deadline");
            it = pendingSimu5gDeliveries.erase(it);
        }
        else {
            ++it;
        }
    }
    for (auto it = pendingSimu5gPc5Transmissions.begin();
         it != pendingSimu5gPc5Transmissions.end();) {
        if (it->second.work != work) {
            ++it;
            continue;
        }
        const std::vector<int> receivers(
            it->second.expectedReceiverIndexes.begin(),
            it->second.expectedReceiverIndexes.end());
        for (const int receiverIndex : receivers) {
            markSimu5gPc5Drop(
                it->second,
                receiverIndex,
                "sync_tick_deadline");
        }
        it = pendingSimu5gPc5Transmissions.erase(it);
    }
    for (auto it = pendingVeinsTransmissions.begin();
         it != pendingVeinsTransmissions.end();) {
        if (it->second.work != work) {
            ++it;
            continue;
        }
        const std::vector<int> receivers(
            it->second.expectedReceivers.begin(),
            it->second.expectedReceivers.end());
        for (const int receiverId : receivers) {
            markVeinsDrop(it->second, receiverId, "sync_tick_deadline");
        }
        it = pendingVeinsTransmissions.erase(it);
    }
}

void MetsrVeinsBridge::handleSyncTickBoundary(omnetpp::cMessage* message)
{
    std::unique_ptr<omnetpp::cMessage> cleanup(message);
    auto* holder = static_cast<SyncWorkHolder*>(message->getContextPointer());
    std::shared_ptr<SyncWork> work = holder != nullptr ? holder->work : nullptr;
    delete holder;
    if (!work) {
        return;
    }
    work->boundaryEvent = nullptr;
    expireSyncWorkDeliveries(work);
    if (work->outstandingDeliveries != 0) {
        work->outstandingDeliveries = 0;
        completeSyncWork(
            work,
            "error",
            "sync_tick reached its boundary with untracked pending deliveries");
        return;
    }
    completeSyncWork(work);
}

void MetsrVeinsBridge::runAbstractEventBackend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
    const int tick = work->tick;
    std::map<int, json> vehicleById;
    for (const auto& vehicle : vehicles) {
        int id = entityId(vehicle, "vehicle_id", 0);
        if (id != 0) {
            vehicleById[id] = vehicle;
        }
    }

    std::vector<std::pair<json, int>> logicalLinks;
    for (const auto& message : messages) {
        const int senderId = bsmSenderId(message);
        std::set<int> receivers;
        for (const int receiverId : explicitExpectedReceivers(message)) {
            receivers.insert(receiverId);
        }
        const int explicitReceiver = bsmReceiverId(message);
        if (receivers.empty() && explicitReceiver > 0) {
            receivers.insert(explicitReceiver);
        }
        if (receivers.empty()) {
            for (const auto& vehicle : vehicleById) {
                if (vehicle.first != senderId) {
                    receivers.insert(vehicle.first);
                }
            }
        }
        for (const int receiverId : receivers) {
            if (receiverId != senderId && vehicleById.find(receiverId) != vehicleById.end()) {
                logicalLinks.emplace_back(message, receiverId);
            }
        }
    }

    std::map<int, int> receiverLoad;
    for (const auto& link : logicalLinks) {
        receiverLoad[link.second] += 1;
    }

    std::map<int, int> receiverQueuePosition;
    const double generationTimeS = omnetpp::simTime().dbl();

    for (const auto& link : logicalLinks) {
        const json& message = link.first;
        const int senderId = bsmSenderId(message);
        const int receiverId = link.second;
        if (senderId == 0 || receiverId == 0) {
            continue;
        }

        const int load = std::max(1, receiverLoad[receiverId]);
        const int queuePosition = receiverQueuePosition[receiverId]++;
        const int payloadBytes = bsmPayloadBytes(message);
        double distance = 0.0;
        auto senderIt = vehicleById.find(senderId);
        auto receiverIt = vehicleById.find(receiverId);
        if (senderIt != vehicleById.end() && receiverIt != vehicleById.end()) {
            distance = distanceM(senderIt->second, receiverIt->second);
        }

        const double per = packetErrorRate(load, distance);
        const double deliveryProbability = std::max(0.0, 1.0 - per);
        const bool delivered = deliveryProbability > 0.0 && uniform(0.0, 1.0) <= deliveryProbability;
        const double scheduledDelayMs = scheduledLatencyMs(load, queuePosition, payloadBytes, distance);
        const double propagationMs =
            SPEED_OF_LIGHT_MPS > 0.0 ? distance / SPEED_OF_LIGHT_MPS * 1000.0 : 0.0;
        const double distanceLatencyMs = std::max(0.0, distanceLatencyUsPerM) * distance / 1000.0;
        const double txTimeS = bsmTxTimeS(message, generationTimeS);
        const int explicitReceiver = bsmReceiverId(message);
        const std::string transmissionId =
            messageIdValue(message, tick, senderId, explicitReceiver);
        const bool broadcast = explicitReceiver <= 0 ||
            message.find("expected_receiver_ids") != message.end();
        const std::string messageId = broadcast
            ? linkMessageId(transmissionId, receiverId)
            : transmissionId;
        const double distanceLossComponent =
            communicationRangeM > 0.0 && distanceLossAtRange > 0.0
                ? distanceLossAtRange * std::pow(std::max(0.0, distance) / communicationRangeM, 2.0)
                : 0.0;

        json metric = {
            {"tick", tick},
            {"message_id", messageId},
            {"transmission_id", transmissionId},
            {"tx_time_s", txTimeS},
            {"sender_id", senderId},
            {"receiver_id", receiverId},
            {"message_name", bsmMessageName(message)},
            {"message_standard", bsmMessageStandard(message)},
            {"message_count", bsmMessageCount(message)},
            {"distance_m", distance},
            {"propagation_delay_ms", propagationMs},
            {"distance_latency_ms", distanceLatencyMs},
            {"distance_loss_component", std::min(0.95, distanceLossComponent)},
            {"generation_time_s", generationTimeS},
            {"scheduled_delay_ms", scheduledDelayMs},
            {"packet_error_rate", per},
            {"delivery_probability", deliveryProbability},
            {"channel_busy_ratio", std::min(1.0, load / 1000.0)},
            {"receiver_load", load},
            {"receiver_queue_position", queuePosition},
            {"payload_bytes", payloadBytes},
            {"radio_mode", bsmRadioMode(message, radioAccess)},
            {"attacked", bsmAttackValue(message, "attacked", "attacked", json(false))},
            {"attack_id", bsmAttackString(message, "attack_id", "attackId")},
            {"attack_type", bsmAttackString(message, "attack_type", "attackType")},
            {"delivered", delivered},
            {"physical_transmission_count", 1},
            {"logical_link_evaluation", broadcast},
        };
        addBridgeMetadata(metric);

        const double remainingTickMs = std::max(
            0.0,
            (work->tickEndTime - omnetpp::simTime()).dbl() * 1000.0);
        const bool missesTickBoundary =
            delivered && scheduledDelayMs > remainingTickMs;
        if (!delivered || missesTickBoundary) {
            metric["delivered"] = false;
            metric["latency_ms"] = nullptr;
            metric["drop_reason"] = missesTickBoundary
                ? "sync_tick_deadline"
                : communicationRangeM > 0.0 && distance > communicationRangeM
                    ? "out_of_range"
                    : "contention_loss";
            work->metrics.push_back(metric);
            continue;
        }

        auto delivery = new PacketDelivery();
        delivery->work = work;
        delivery->message = message;
        delivery->metric = metric;
        delivery->tick = tick;
        delivery->senderId = senderId;
        delivery->receiverId = receiverId;
        delivery->generationTimeS = generationTimeS;

        auto event = new omnetpp::cMessage("packet-delivery");
        event->setKind(KIND_PACKET_DELIVERY);
        event->setContextPointer(delivery);
        work->outstandingDeliveries += 1;
        scheduleAt(omnetpp::simTime() + scheduledDelayMs / 1000.0, event);
    }

    if (work->outstandingDeliveries == 0) {
        return;
    }
}

void MetsrVeinsBridge::handlePacketDelivery(omnetpp::cMessage* message)
{
    auto* delivery = static_cast<PacketDelivery*>(message->getContextPointer());
    std::unique_ptr<PacketDelivery> cleanup(delivery);
    std::unique_ptr<omnetpp::cMessage> eventCleanup(message);
    if (delivery == nullptr || delivery->work == nullptr) {
        return;
    }

    auto work = delivery->work;
    const double receiveTimeS = omnetpp::simTime().dbl();
    const double latencyMs = (receiveTimeS - delivery->generationTimeS) * 1000.0;

    json metric = delivery->metric;
    metric["latency_ms"] = latencyMs;
    metric["receive_time_s"] = receiveTimeS;
    work->metrics.push_back(metric);

    json deliveredMessage = delivery->message;
    deliveredMessage["tick"] = delivery->tick;
    deliveredMessage["message_id"] = metric["message_id"];
    deliveredMessage["transmission_id"] = metric["transmission_id"];
    deliveredMessage["tx_time_s"] = metric["tx_time_s"];
    deliveredMessage["sender_id"] = delivery->senderId;
    deliveredMessage["receiver_id"] = delivery->receiverId;
    deliveredMessage["generation_time_s"] = delivery->generationTimeS;
    deliveredMessage["receive_time_s"] = receiveTimeS;
    deliveredMessage["latency_ms"] = latencyMs;
    deliveredMessage["packet_error_rate"] = metric["packet_error_rate"];
    deliveredMessage["delivery_probability"] = metric["delivery_probability"];
    deliveredMessage["receiver_load"] = metric["receiver_load"];
    deliveredMessage["receiver_queue_position"] = metric["receiver_queue_position"];
    deliveredMessage["radio_mode"] = metric["radio_mode"];
    deliveredMessage["attacked"] = metric["attacked"];
    deliveredMessage["attack_id"] = metric["attack_id"];
    deliveredMessage["attack_type"] = metric["attack_type"];
    addBridgeMetadata(deliveredMessage);
    work->received.push_back(deliveredMessage);

    work->outstandingDeliveries -= 1;
    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
}

void MetsrVeinsBridge::completeSyncWork(
    const std::shared_ptr<SyncWork>& work,
    const std::string& status,
    const std::string& message)
{
    {
        std::lock_guard<std::mutex> lock(work->mutex);
        if (work->done) {
            return;
        }
    }
    if (work->timeoutEvent != nullptr) {
        auto* holder = static_cast<SyncWorkHolder*>(work->timeoutEvent->getContextPointer());
        delete holder;
        work->timeoutEvent->setContextPointer(nullptr);
        cancelAndDelete(work->timeoutEvent);
        work->timeoutEvent = nullptr;
    }
    if (status == "ok" &&
        work->boundaryEvent != nullptr &&
        omnetpp::simTime() < work->tickEndTime) {
        work->completionPending = true;
        return;
    }
    if (work->boundaryEvent != nullptr) {
        auto* holder = static_cast<SyncWorkHolder*>(
            work->boundaryEvent->getContextPointer());
        delete holder;
        work->boundaryEvent->setContextPointer(nullptr);
        cancelAndDelete(work->boundaryEvent);
        work->boundaryEvent = nullptr;
    }
    work->completionPending = false;
    activeSyncWorkSet.erase(work);
    activeSyncWorks = std::max(0, activeSyncWorks - 1);
    if (running &&
        activeSyncWorks == 0 &&
        keepAlive != nullptr &&
        keepAlive->isScheduled() &&
        keepAlive->getArrivalTime() > omnetpp::simTime()) {
        cancelEvent(keepAlive);
        scheduleAt(omnetpp::simTime(), keepAlive);
    }
    json data = {
        {"received_bsms", work->received},
        {"link_metrics", work->metrics},
        {"attack_events", work->attackEvents},
        {"duration_s", work->durationS},
        {"tick_start_time_s", work->tickStartTime.dbl()},
        {"tick_end_time_s", work->tickEndTime.dbl()},
    };
    const json metadata = bridgeMetadata(true);
    for (const auto& item : metadata.items()) {
        data[item.key()] = item.value();
    }
    json response = {
        {"type", "sync_tick_result"},
        {"request_id", work->requestId},
        {"status", status},
        {"data", data},
    };
    if (!message.empty()) {
        response["message"] = message;
    }

    {
        std::lock_guard<std::mutex> lock(work->mutex);
        work->response = response;
        work->done = true;
    }
    work->cv.notify_all();
}

double MetsrVeinsBridge::scheduledLatencyMs(
    int receiverLoad,
    int queuePosition,
    int payloadBytes,
    double distanceM)
{
    const double queueDelayMs = perMessageLatencyMs * std::max(0, queuePosition);
    const double payloadProcessingMs = perPayloadByteLatencyUs * payloadBytes / 1000.0;
    const double serializationMs =
        bitrateMbps > 0.0 ? (payloadBytes * 8.0) / (bitrateMbps * 1000000.0) * 1000.0 : 0.0;
    const double propagationMs =
        SPEED_OF_LIGHT_MPS > 0.0 ? distanceM / SPEED_OF_LIGHT_MPS * 1000.0 : 0.0;
    const double distanceModelMs = std::max(0.0, distanceLatencyUsPerM) * distanceM / 1000.0;
    const double contentionWindowSlots =
        std::min(1023.0, 15.0 + std::max(0, receiverLoad - 1) / 2.0);
    const double backoffMs =
        macSlotTimeMs > 0.0 ? uniform(0.0, contentionWindowSlots) * macSlotTimeMs : 0.0;
    const double jitterMs = maxJitterMs > 0.0 ? uniform(0.0, maxJitterMs) : 0.0;
    return std::max(
        0.0,
        baseLatencyMs + queueDelayMs + payloadProcessingMs + serializationMs +
            propagationMs + distanceModelMs + backoffMs + jitterMs);
}

double MetsrVeinsBridge::packetErrorRate(int receiverLoad, double distanceM) const
{
    if (communicationRangeM > 0.0 && distanceM > communicationRangeM) {
        return 1.0;
    }
    const double contentionLoss = contentionLossSlope * std::max(0, receiverLoad - 1);
    double distanceLoss = 0.0;
    if (communicationRangeM > 0.0 && distanceLossAtRange > 0.0) {
        const double normalizedDistance = std::max(0.0, distanceM) / communicationRangeM;
        distanceLoss = distanceLossAtRange * normalizedDistance * normalizedDistance;
    }
    return std::min(0.95, contentionLoss + distanceLoss);
}
