#pragma once

namespace metsr::veinsbridge {

constexpr int KIND_KEEP_ALIVE = 1;
constexpr int KIND_PACKET_DELIVERY = 2;
constexpr int KIND_SYNC_TICK_BOUNDARY = 3;
constexpr int KIND_SIMU5G_BSM_REQUEST = 1001;
constexpr int KIND_SIMU5G_RX_REPORT = 1002;
constexpr int KIND_SIMU5G_MOBILITY_UPDATE = 1003;
constexpr int KIND_SIMU5G_SYNC_TIMEOUT = 1004;
constexpr int KIND_VEINS_BSM_REQUEST = 2001;
constexpr int KIND_VEINS_RX_REPORT = 2002;
constexpr int KIND_VEINS_MOBILITY_UPDATE = 2003;
constexpr int KIND_VEINS_SYNC_TIMEOUT = 2004;

constexpr const char* SIMU5G_BRIDGE_GATE = "bridgeIn";
constexpr const char* SIMU5G_REPORT_GATE = "sim5gReportIn";
constexpr const char* VEINS_BRIDGE_GATE = "bridgeIn";
constexpr const char* VEINS_REPORT_GATE = "veinsReportIn";

} // namespace metsr::veinsbridge
