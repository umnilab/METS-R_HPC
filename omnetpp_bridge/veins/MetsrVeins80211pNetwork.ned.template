import org.car2x.veins.base.connectionManager.ConnectionManager;
import org.car2x.veins.base.modules.BaseWorldUtility;
import org.car2x.veins.modules.world.annotations.AnnotationManager;

network MetsrVeins80211pNetwork
{
    parameters:
        double playgroundSizeX @unit(m) = default(30000m);
        double playgroundSizeY @unit(m) = default(30000m);
        double playgroundSizeZ @unit(m) = default(1000m);
        @display("bgb=30000,30000");
    submodules:
        annotations: AnnotationManager {
            parameters:
                draw = false;
        }
        connectionManager: ConnectionManager {
        }
        world: BaseWorldUtility {
            parameters:
                playgroundSizeX = parent.playgroundSizeX;
                playgroundSizeY = parent.playgroundSizeY;
                playgroundSizeZ = parent.playgroundSizeZ;
                use2D = false;
        }
        bridge: MetsrVeinsBridge {
        }
    connections allowunconnected:
}
