#pragma once

#ifdef METSR_WITH_VEINS

#include "veins/modules/application/ieee80211p/DemoBaseApplLayer.h"

namespace metsr::veinsbridge {

class MetsrVeinsApp : public veins::DemoBaseApplLayer {
  private:
    int externalVehicleId = 0;
    omnetpp::cModule* bridge = nullptr;

  protected:
    void initialize(int stage) override;
    void handleMessage(omnetpp::cMessage* message) override;
    void onBSM(veins::DemoSafetyMessage* bsm) override;

  private:
    void injectBsm(omnetpp::cMessage* request);
    omnetpp::cModule* resolveBridge();
};

} // namespace metsr::veinsbridge

#endif // METSR_WITH_VEINS
