package info.jerrinot.hazelcast.longmaxupdater.spi;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class UpdateOperation extends LongMaxUpdaterBackupAwareOperation {
    private long x;

    public UpdateOperation() {

    }

    public UpdateOperation(String name, long x) {
        super(name);
        this.x = x;
    }

    @Override
    public void run() throws Exception {
        shouldBackup = (getNumber().update(x) == x);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(x);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        x = in.readLong();
    }

    @Override
    public Operation getBackupOperation() {
        return new UpdateBackupOperation(name, x);
    }
}
