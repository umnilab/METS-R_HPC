#pragma once

#ifdef METSR_WITH_VEINS

#include "veins/base/modules/BaseMobility.h"

namespace metsr::veinsbridge {

/** Mobility controlled by METS-R instead of SUMO/TraCI. */
class MetsrVeinsExternalMobility : public veins::BaseMobility {
  private:
    int externalVehicleId = 0;
    veins::Coord externalVelocity = veins::Coord::ZERO;

  protected:
    void initialize(int stage) override;
    void handleMessage(omnetpp::cMessage* message) override;
    void makeMove() override;
    void fixIfHostGetsOutside() override;
    void refreshDisplay() const override;

  public:
    int getExternalVehicleId() const { return externalVehicleId; }
    veins::Coord getCurrentSpeed() const override { return externalVelocity; }
};

} // namespace metsr::veinsbridge

#endif // METSR_WITH_VEINS
