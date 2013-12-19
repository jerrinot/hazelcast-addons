package info.jerrinot.hazelcast.longmaxupdater.spi;

public class MaxOperation extends LongMaxUpdaterBaseOperation {
    public MaxOperation() {

    }

    public MaxOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getNumber().max();
    }

}
