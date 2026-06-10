#include <omnetpp.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <iostream>
#include <map>
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
    std::string radioAccess = "cv2x";

    std::thread serverThread;
    std::atomic<bool> running {false};
    int serverFd = -1;
    std::mutex socketMutex;
    omnetpp::cMessage* keepAlive = nullptr;

  protected:
    void initialize() override;
    void handleMessage(omnetpp::cMessage* message) override;
    void finish() override;

  private:
    void serverLoop();
    void handleConnection(int clientFd);
    json handleRequest(const json& request);
    json handleSyncTick(const json& request);
    void closeServerSocket();
    double targetLoadLatencyMs(int receiverLoad, int payloadBytes) const;
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
    radioAccess = par("radioAccess").stringValue();

    running = true;
    keepAlive = new omnetpp::cMessage("bridge-keep-alive");
    scheduleAt(omnetpp::simTime() + 1, keepAlive);
    serverThread = std::thread(&MetsrVeinsBridge::serverLoop, this);
    EV_INFO << "METS-R Veins bridge starting on " << bridgeHost << ":" << bridgePort << "\n";
}

void MetsrVeinsBridge::handleMessage(omnetpp::cMessage* message)
{
    if (message == keepAlive && running) {
        scheduleAt(omnetpp::simTime() + 1, keepAlive);
    }
}

void MetsrVeinsBridge::finish()
{
    running = false;
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
        return {
            {"type", "hello_result"},
            {"request_id", requestId},
            {"status", "ok"},
            {"protocol", PROTOCOL_NAME},
            {"version", PROTOCOL_VERSION},
        };
    }
    if (type == "ping") {
        return {
            {"type", "ping_result"},
            {"request_id", requestId},
            {"status", "ok"},
        };
    }
    if (type == "reset") {
        return {
            {"type", "reset_result"},
            {"request_id", requestId},
            {"status", "ok"},
        };
    }
    if (type == "sync_tick") {
        return handleSyncTick(request);
    }

    return {
        {"type", type + "_result"},
        {"request_id", requestId},
        {"status", "error"},
        {"message", "unsupported request type: " + type},
    };
}

double MetsrVeinsBridge::targetLoadLatencyMs(int receiverLoad, int payloadBytes) const
{
    const double loadPenalty = perMessageLatencyMs * std::max(0, receiverLoad - 1);
    const double payloadPenalty = perPayloadByteLatencyUs * payloadBytes / 1000.0;
    return baseLatencyMs + loadPenalty + payloadPenalty;
}

json MetsrVeinsBridge::handleSyncTick(const json& request)
{
    const int requestId = intValue(request, "request_id", 0);
    const int tick = intValue(request, "tick", 0);
    const auto vehicles = request.value("vehicles", json::array());
    const auto messages = request.value("bsm_messages", json::array());

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

    json received = json::array();
    json metrics = json::array();
    json attackEvents = json::array();

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
        const int payloadBytes = intValue(message, "payload_bytes", 0);
        const double latencyMs = targetLoadLatencyMs(load, payloadBytes);
        const double packetErrorRate = std::min(0.95, contentionLossSlope * std::max(0, load - 1));
        const double deliveryProbability = std::max(0.0, 1.0 - packetErrorRate);
        const bool delivered = deliveryProbability > 0.0;
        double distance = 0.0;
        auto senderIt = vehicleById.find(senderId);
        auto receiverIt = vehicleById.find(receiverId);
        if (senderIt != vehicleById.end() && receiverIt != vehicleById.end()) {
            distance = distanceM(senderIt->second, receiverIt->second);
        }

        json metric = {
            {"tick", tick},
            {"sender_id", senderId},
            {"receiver_id", receiverId},
            {"distance_m", distance},
            {"latency_ms", latencyMs},
            {"packet_error_rate", packetErrorRate},
            {"delivery_probability", deliveryProbability},
            {"channel_busy_ratio", std::min(1.0, load / 1000.0)},
            {"receiver_load", load},
            {"payload_bytes", payloadBytes},
            {"radio_access", radioAccess},
            {"delivered", delivered},
            {"network_model", "omnetpp_veins_bridge_load_model"},
        };
        metrics.push_back(metric);

        if (delivered) {
            json deliveredMessage = message;
            deliveredMessage["tick"] = tick;
            deliveredMessage["sender_id"] = senderId;
            deliveredMessage["receiver_id"] = receiverId;
            deliveredMessage["latency_ms"] = latencyMs;
            deliveredMessage["packet_error_rate"] = packetErrorRate;
            deliveredMessage["delivery_probability"] = deliveryProbability;
            deliveredMessage["radio_access"] = radioAccess;
            deliveredMessage["receiver_load"] = load;
            deliveredMessage["network_model"] = "omnetpp_veins_bridge_load_model";
            received.push_back(deliveredMessage);
        }
    }

    return {
        {"type", "sync_tick_result"},
        {"request_id", requestId},
        {"status", "ok"},
        {"data", {
            {"received_bsms", received},
            {"link_metrics", metrics},
            {"attack_events", attackEvents},
            {"bridge_model", "omnetpp_veins_bridge_load_model"},
        }},
    };
}
