package cloud.orbit.actors.runtime;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.runtime.NodeCapabilities.NodeState;
import cloud.orbit.concurrent.Task;

public class NodeInfo
{
    boolean active;
    final NodeAddress address;
    NodeState state = NodeState.RUNNING;
    NodeCapabilities nodeCapabilities;
    boolean cannotHostActors;
    final ConcurrentHashMap<String, Integer> canActivate = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Task<Void>> canActivate2 = new ConcurrentHashMap<>();
    final Set<String> canActivatePending = ConcurrentHashMap.newKeySet();
    String nodeName;

    public NodeInfo(final NodeAddress address)
    {
        this.address = address;
    }

    public boolean getActive()
    {
        return this.active;
    }

    public NodeAddress getAddress()
    {
        return this.address;
    }
}
