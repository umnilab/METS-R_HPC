#pragma once

namespace metsr::veinsbridge {

constexpr int KIND_KEEP_ALIVE = 1;
constexpr int KIND_PACKET_DELIVERY = 2;
constexpr int KIND_SIMU5G_BSM_REQUEST = 1001;
constexpr int KIND_SIMU5G_RX_REPORT = 1002;
constexpr int KIND_SIMU5G_MOBILITY_UPDATE = 1003;
constexpr int KIND_SIMU5G_SYNC_TIMEOUT = 1004;

constexpr const char* SIMU5G_BRIDGE_GATE = "bridgeIn";
constexpr const char* SIMU5G_REPORT_GATE = "sim5gReportIn";

} // namespace metsr::veinsbridge
