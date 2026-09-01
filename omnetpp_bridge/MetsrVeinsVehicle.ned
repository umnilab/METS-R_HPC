import org.car2x.veins.modules.nic.Nic80211p;

module MetsrVeinsVehicle
{
    parameters:
        int externalVehicleId;
        @display("i=vehicle/car");
    gates:
        input veinsradioIn;
    submodules:
        appl: MetsrVeinsApp {
            parameters:
                externalVehicleId = parent.externalVehicleId;
                @display("p=70,55");
        }
        nic: Nic80211p {
            parameters:
                @display("p=70,145");
        }
        veinsmobility: MetsrVeinsExternalMobility {
            parameters:
                @display("p=145,145");
        }
    connections:
        nic.upperLayerOut --> appl.lowerLayerIn;
        nic.upperLayerIn <-- appl.lowerLayerOut;
        nic.upperControlOut --> appl.lowerControlIn;
        nic.upperControlIn <-- appl.lowerControlOut;
        veinsradioIn --> nic.radioIn;
}
