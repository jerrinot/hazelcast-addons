package info.jerrinot.hazelcast.longmaxupdater.spi;

public class ResetBackupOperation extends LongMaxUpdaterBaseOperation{

    public ResetBackupOperation() {
    }

    public ResetBackupOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        LongMaxWrapper number = getNumber();
        number.reset();
    }
}
