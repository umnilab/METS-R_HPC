import org.car2x.veins.modules.application.ieee80211p.DemoBaseApplLayer;

simple MetsrVeinsApp extends DemoBaseApplLayer
{
    parameters:
        @class(metsr::veinsbridge::MetsrVeinsApp);
        int externalVehicleId;
        string bridgeModulePath = default("");
        double bridgeInjectionSpread @unit(s) = default(20ms);
        sendBeacons = false;
        dataOnSch = false;
    gates:
        input bridgeIn @directIn;
}
