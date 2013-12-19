package info.jerrinot.hazelcast.longmaxupdater.spi;

import com.hazelcast.spi.Operation;

public class MaxThenResetOperation extends LongMaxUpdaterBackupAwareOperation {

    public MaxThenResetOperation() {

    }

    public MaxThenResetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        LongMaxWrapper number = getNumber();
        returnValue = number.maxThenReset();
    }

    @Override
    public Operation getBackupOperation() {
        return new ResetBackupOperation(name);
    }
}
