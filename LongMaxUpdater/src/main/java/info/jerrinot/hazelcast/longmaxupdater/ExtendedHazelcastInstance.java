package info.jerrinot.hazelcast.longmaxupdater;

import com.hazelcast.core.HazelcastInstance;

public interface ExtendedHazelcastInstance extends HazelcastInstance {
    ILongMaxUpdater getLongMaxUpdater(String name);
}
