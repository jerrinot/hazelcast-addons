package info.jerrinot.hazelcast.longmaxupdater;

import com.hazelcast.core.DistributedObject;

public interface ILongMaxUpdater extends DistributedObject {

    long max();

    void update(long x);

    long maxThenReset();

    void reset();

}
