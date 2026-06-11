#include <omnetpp.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstring>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
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

using json = nlohmann::json;

namespace {

constexpr const char* PROTOCOL_NAME = "metsr-veins-jsonl";
constexpr int PROTOCOL_VERSION = 1;
constexpr const char* DEFAULT_NETWORK_MODEL = "omnetpp_event_wireless_queue_model";
constexpr int KIND_KEEP_ALIVE = 1;
constexpr int KIND_PACKET_DELIVERY = 2;
constexpr double SPEED_OF_LIGHT_MPS = 299792458.0;

struct SyncWork {
    int requestId = 0;
    int tick = 0;
    json request;
    json received = json::array();
    json metrics = json::array();
    json attackEvents = json::array();
    int outstandingDeliveries = 0;
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

std::string messageIdValue(const json& record, int tick, int senderId, int receiverId)
{
    const std::string explicitId = stringValue(record, "message_id");
    if (!explicitId.empty()) {
        return explicitId;
    }
    const int messageCount = intValue(record, "message_count", 0);
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
    std::string bridgeBackend = "abstract_omnetpp";
    std::string backendImplementation = "abstract_event_profile";
    std::string networkModel = DEFAULT_NETWORK_MODEL;
    std::string backendNote;
    std::string radioAccess = "cv2x";
    bool logRequests = true;

    std::thread serverThread;
    std::atomic<bool> running {false};
    int serverFd = -1;
    std::mutex socketMutex;
    std::mutex pendingMutex;
    std::deque<std::shared_ptr<SyncWork>> pendingSyncRequests;
    int activeSyncWorks = 0;
    omnetpp::cMessage* keepAlive = nullptr;

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
    void recordAttackEvents(const std::shared_ptr<SyncWork>& work);
    void runActiveBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    void runAbstractEventBackend(
        const std::shared_ptr<SyncWork>& work,
        const json& vehicles,
        const json& messages);
    void handlePacketDelivery(omnetpp::cMessage* message);
    void completeSyncWork(const std::shared_ptr<SyncWork>& work);
    void closeServerSocket();
    json bridgeMetadata(bool includeNote = false) const;
    void addBridgeMetadata(json& record, bool includeNote = false) const;
    double scheduledLatencyMs(int receiverLoad, int queuePosition, int payloadBytes, double distanceM);
    double packetErrorRate(int receiverLoad, double distanceM) const;
};

Define_Module(MetsrVeinsBridge);

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
    bridgeBackend = par("bridgeBackend").stringValue();
    backendImplementation = par("backendImplementation").stringValue();
    networkModel = par("networkModel").stringValue();
    backendNote = par("backendNote").stringValue();
    radioAccess = par("radioAccess").stringValue();
    logRequests = par("logRequests").boolValue();

    running = true;
    keepAlive = new omnetpp::cMessage("bridge-keep-alive");
    keepAlive->setKind(KIND_KEEP_ALIVE);
    scheduleAt(omnetpp::simTime() + pollIntervalMs / 1000.0, keepAlive);
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
            scheduleAt(omnetpp::simTime() + pollIntervalMs / 1000.0, keepAlive);
        }
        return;
    }

    if (message->getKind() == KIND_PACKET_DELIVERY) {
        handlePacketDelivery(message);
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
    closeServerSocket();
    if (serverThread.joinable()) {
        serverThread.join();
    }
    if (keepAlive != nullptr) {
        cancelAndDelete(keepAlive);
        keepAlive = nullptr;
    }
}

void MetsrVeinsBridge::closeServerSocket()
{
    std::lock_guard<std::mutex> lock(socketMutex);
    if (serverFd >= 0) {
        ::shutdown(serverFd, SHUT_RDWR);
        ::close(serverFd);
        serverFd = -1;
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
        handleConnection(clientFd);
        ::close(clientFd);
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
            {"status", "ok"},
        };
        addBridgeMetadata(response);
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
    const auto vehicles = work->request.value("vehicles", json::array());
    const auto messages = work->request.value("bsm_messages", json::array());

    recordAttackEvents(work);
    runActiveBackend(work, vehicles, messages);

    if (work->outstandingDeliveries == 0) {
        completeSyncWork(work);
    }
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

void MetsrVeinsBridge::runActiveBackend(
    const std::shared_ptr<SyncWork>& work,
    const json& vehicles,
    const json& messages)
{
    if (backendImplementation == "abstract_event_profile" ||
        backendImplementation == "abstract_profile_pending_full_veins" ||
        backendImplementation == "abstract_profile_pending_simu5g") {
        runAbstractEventBackend(work, vehicles, messages);
        return;
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

    std::map<int, int> receiverLoad;
    for (const auto& message : messages) {
        int receiverId = entityId(message, "receiver_id", 0);
        if (receiverId == 0 && hasKey(message, "target_vehicle_id")) {
            receiverId = intValue(message, "target_vehicle_id", 0);
        }
        if (receiverId != 0) {
            receiverLoad[receiverId] += 1;
        }
    }

    std::map<int, int> receiverQueuePosition;
    const double generationTimeS = omnetpp::simTime().dbl();

    for (const auto& message : messages) {
        const int senderId = entityId(message, "sender_id", entityId(message, "vehicle_id", 0));
        int receiverId = entityId(message, "receiver_id", 0);
        if (receiverId == 0 && hasKey(message, "target_vehicle_id")) {
            receiverId = intValue(message, "target_vehicle_id", 0);
        }
        if (senderId == 0 || receiverId == 0) {
            continue;
        }

        const int load = std::max(1, receiverLoad[receiverId]);
        const int queuePosition = receiverQueuePosition[receiverId]++;
        const int payloadBytes = intValue(message, "payload_bytes", 0);
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
        const double txTimeS = numberValue(message, "tx_time_s", generationTimeS);
        const std::string messageId = messageIdValue(message, tick, senderId, receiverId);
        const double distanceLossComponent =
            communicationRangeM > 0.0 && distanceLossAtRange > 0.0
                ? distanceLossAtRange * std::pow(std::max(0.0, distance) / communicationRangeM, 2.0)
                : 0.0;

        json metric = {
            {"tick", tick},
            {"message_id", messageId},
            {"tx_time_s", txTimeS},
            {"sender_id", senderId},
            {"receiver_id", receiverId},
            {"message_name", stringValue(message, "message_name")},
            {"message_standard", stringValue(message, "message_standard")},
            {"message_count", intValue(message, "message_count", 0)},
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
            {"radio_mode", stringValue(message, "radio_mode", radioAccess)},
            {"attacked", hasKey(message, "attacked") ? message["attacked"] : false},
            {"attack_id", stringValue(message, "attack_id")},
            {"attack_type", stringValue(message, "attack_type")},
            {"delivered", delivered},
        };
        addBridgeMetadata(metric);

        if (!delivered) {
            metric["latency_ms"] = nullptr;
            metric["drop_reason"] =
                communicationRangeM > 0.0 && distance > communicationRangeM
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

void MetsrVeinsBridge::completeSyncWork(const std::shared_ptr<SyncWork>& work)
{
    activeSyncWorks = std::max(0, activeSyncWorks - 1);
    json data = {
        {"received_bsms", work->received},
        {"link_metrics", work->metrics},
        {"attack_events", work->attackEvents},
    };
    const json metadata = bridgeMetadata(true);
    for (const auto& item : metadata.items()) {
        data[item.key()] = item.value();
    }
    json response = {
        {"type", "sync_tick_result"},
        {"request_id", work->requestId},
        {"status", "ok"},
        {"data", data},
    };

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
