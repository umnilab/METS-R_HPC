#ifdef METSR_WITH_SIMU5G

#include <cmath>
#include <memory>

#include <omnetpp.h>

#include <inet/common/geometry/common/Quaternion.h>
#include <inet/mobility/base/StationaryMobilityBase.h>

#include "MetsrVeinsBridgeProtocol.h"

namespace metsr::veinsbridge::sim5g {

using namespace omnetpp;
using namespace inet;

class MetsrExternalMobility : public StationaryMobilityBase {
  private:
    Coord lastExternalVelocity = Coord::ZERO;
    int vehicleId = 0;

  protected:
    int numInitStages() const override { return inet::NUM_INIT_STAGES; }
    void initialize(int stage) override;
    void handleMessage(cMessage* message) override;
    void refreshDisplay() const override;

  public:
    const Coord& getCurrentVelocity() override { return lastExternalVelocity; }
    double getMaxSpeed() const override { return lastExternalVelocity.length(); }
};

Define_Module(MetsrExternalMobility);

void MetsrExternalMobility::initialize(int stage)
{
    StationaryMobilityBase::initialize(stage);
}

void MetsrExternalMobility::handleMessage(cMessage* message)
{
    std::unique_ptr<cMessage> cleanup(message);
    if (message->getKind() != metsr::veinsbridge::KIND_SIMU5G_MOBILITY_UPDATE) {
        throw cRuntimeError("MetsrExternalMobility only accepts bridge mobility updates");
    }

    vehicleId = message->hasPar("vehicle_id")
        ? static_cast<int>(message->par("vehicle_id").longValue())
        : vehicleId;
    const double x = message->hasPar("x") ? message->par("x").doubleValue() : lastPosition.x;
    const double y = message->hasPar("y") ? message->par("y").doubleValue() : lastPosition.y;
    const double z = message->hasPar("z") ? message->par("z").doubleValue() : lastPosition.z;
    const double speedMps = message->hasPar("speed_mps") ? message->par("speed_mps").doubleValue() : 0.0;
    const double headingDeg = message->hasPar("heading_deg") ? message->par("heading_deg").doubleValue() : 0.0;

    lastPosition = Coord(x, y, z);
    const double headingRad = headingDeg * 3.14159265358979323846 / 180.0;
    lastExternalVelocity = Coord(std::cos(headingRad) * speedMps, std::sin(headingRad) * speedMps, 0.0);
    lastOrientation = Quaternion::IDENTITY;

    checkPosition();
    emitMobilityStateChangedSignal();
    if (par("updateDisplayString")) {
        updateDisplayStringFromMobilityState();
    }
}

void MetsrExternalMobility::refreshDisplay() const
{
    char buffer[128];
    snprintf(
        buffer,
        sizeof(buffer),
        "vehicle_id: %d\np: %.1f %.1f %.1f\nv: %.1f m/s",
        vehicleId,
        lastPosition.x,
        lastPosition.y,
        lastPosition.z,
        lastExternalVelocity.length());
    getDisplayString().setTagArg("t", 0, buffer);
    if (par("updateDisplayString")) {
        updateDisplayStringFromMobilityState();
    }
}

} // namespace metsr::veinsbridge::sim5g

#endif // METSR_WITH_SIMU5G
