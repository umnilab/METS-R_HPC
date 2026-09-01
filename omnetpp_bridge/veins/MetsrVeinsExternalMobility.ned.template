import org.car2x.veins.base.modules.BaseMobility;

simple MetsrVeinsExternalMobility extends BaseMobility
{
    parameters:
        @class(metsr::veinsbridge::MetsrVeinsExternalMobility);
        // Dynamic modules must have concrete values before buildInside(); the
        // bridge overwrites these immediately with the first METS-R position.
        x @mutable = default(0);
        y @mutable = default(0);
        z @mutable = default(0);
    gates:
        input bridgeIn @directIn;
}
