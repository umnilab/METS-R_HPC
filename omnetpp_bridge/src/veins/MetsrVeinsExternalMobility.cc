#ifdef METSR_WITH_VEINS

#include "MetsrVeinsExternalMobility.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "MetsrVeinsBridgeProtocol.h"

namespace metsr::veinsbridge {

using namespace omnetpp;

Define_Module(MetsrVeinsExternalMobility);

void MetsrVeinsExternalMobility::initialize(int stage)
{
    veins::BaseMobility::initialize(stage);
    if (stage == 0 && getParentModule()->hasPar("externalVehicleId")) {
        externalVehicleId = getParentModule()->par("externalVehicleId").intValue();
    }
}

void MetsrVeinsExternalMobility::handleMessage(cMessage* message)
{
    if (message->isSelfMessage()) {
        veins::BaseMobility::handleMessage(message);
        return;
    }
    std::unique_ptr<cMessage> cleanup(message);
    if (message->getKind() != KIND_VEINS_MOBILITY_UPDATE) {
        throw cRuntimeError("Unknown external mobility message");
    }
    if (message->hasPar("vehicle_id")) {
        externalVehicleId = static_cast<int>(message->par("vehicle_id").longValue());
    }
    const veins::Coord current = move.getPositionAt(simTime());
    const double x = message->hasPar("x") ? message->par("x").doubleValue() : current.x;
    const double y = message->hasPar("y") ? message->par("y").doubleValue() : current.y;
    const double z = message->hasPar("z") ? message->par("z").doubleValue() : current.z;
    const double speedMps = std::max(
        0.0,
        message->hasPar("speed_mps") ? message->par("speed_mps").doubleValue() : 0.0);
    const double headingDeg = message->hasPar("heading_deg")
        ? message->par("heading_deg").doubleValue()
        : 0.0;
    constexpr double PI = 3.14159265358979323846;
    const double headingRad = headingDeg * PI / 180.0;
    const veins::Coord orientation(std::cos(headingRad), std::sin(headingRad), 0.0);
    const veins::Coord direction = speedMps > 0.0 ? orientation : veins::Coord::ZERO;

    move.setStart(veins::Coord(x, y, z), simTime());
    move.setOrientationByVector(orientation);
    move.setDirectionByVector(direction);
    move.setSpeed(speedMps);
    externalVelocity = orientation * speedMps;
    updatePosition();
}

void MetsrVeinsExternalMobility::makeMove()
{
    // METS-R is the sole mobility authority.
}

void MetsrVeinsExternalMobility::fixIfHostGetsOutside()
{
    throw cRuntimeError(
        "METS-R vehicle %d is outside the Veins playground; adjust "
        "veinsCoordinateOffset* or playgroundSize*",
        externalVehicleId);
}

void MetsrVeinsExternalMobility::refreshDisplay() const
{
    char displayText[160];
    const veins::Coord position = move.getPositionAt(simTime());
    snprintf(
        displayText,
        sizeof(displayText),
        "METS-R id: %d\npos: %.1f %.1f %.1f\nspeed: %.1f m/s",
        externalVehicleId,
        position.x,
        position.y,
        position.z,
        externalVelocity.length());
    getParentModule()->getDisplayString().setTagArg("t", 0, displayText);
}

} // namespace metsr::veinsbridge

#endif
