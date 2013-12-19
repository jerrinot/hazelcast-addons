package info.jerrinot.hazelcast.longmaxupdater.spi.proxy;

import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;
import info.jerrinot.hazelcast.longmaxupdater.ILongMaxUpdater;
import info.jerrinot.hazelcast.longmaxupdater.spi.*;

import java.util.concurrent.Future;

public class LongMaxUpdaterProxy extends AbstractDistributedObject<LongMaxUpdaterService> implements ILongMaxUpdater {

    private int partitionId;
    private String name;

    public LongMaxUpdaterProxy(String name, NodeEngine nodeEngine, LongMaxUpdaterService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> E invoke(Operation operation) {
        try {
            return (E) getNodeEngine().getOperationService().createInvocationBuilder(LongMaxUpdaterService.SERVICE_NAME, operation, partitionId).build().invoke().get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public long max() {
        Operation operation = new MaxOperation(name);
        return (Long) invoke(operation);
    }

    @Override
    public void update(long x) {
        Operation operation = new UpdateOperation(name, x);
        invoke(operation);
    }

    @Override
    public long maxThenReset() {
        Operation operation = new MaxThenResetOperation(name);
        return (Long) invoke(operation);
    }

    @Override
    public void reset() {
        Operation operation = new ResetOperation(name);
        invoke(operation);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return LongMaxUpdaterService.SERVICE_NAME;
    }
}
