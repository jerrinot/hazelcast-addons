package info.jerrinot.hazelcast.longmaxupdater.spi.longmaxupdater;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import info.jerrinot.hazelcast.longmaxupdater.ILongMaxUpdater;
import info.jerrinot.hazelcast.longmaxupdater.spi.LongMaxUpdaterService;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class LongMaxUpdaterTest extends HazelcastTestSupport {

    @Test
    public void testMaxThenReset() {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newInstances(getConfig())[0];
        ILongMaxUpdater updater = getLongMaxUpdater(instance, "updater");

        long maxValue = Long.MIN_VALUE;
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong();
            maxValue = Math.max(maxValue, value);
            updater.update(value);
        }

        long max = updater.maxThenReset();
        assertEquals(maxValue, max);
        assertEquals(Long.MIN_VALUE, updater.max());
    }

    @Test
    public void testMutipleThreadLongMaxUpdater() throws Exception {
        final HazelcastInstance instance = createHazelcastInstanceFactory(3).newInstances(getConfig())[0];

        final int noOfThreads = 5;
        final String updaterName = "updater";
        final long[] maxArray = new long[noOfThreads];
        final CountDownLatch countDownLatch = new CountDownLatch(noOfThreads);
        final Random random = new Random();
        for (int i =0; i < noOfThreads; i++) {
            new Thread() {
                private int id;
                Thread setId(int id) {
                    this.id = id;
                    return this;
                }
                @Override
                public void run() {
                    ILongMaxUpdater updater = getLongMaxUpdater(instance, updaterName);
                    for (int j = 0; j < 1000; j++) {
                        long value = random.nextLong();
                        updater.update(value);
                        maxArray[id] = Math.max(maxArray[id], value);
                    }
                    countDownLatch.countDown();
                }
            }.setId(i).start();
        }
        countDownLatch.await(50, TimeUnit.SECONDS);

        long maxFromHZ = getLongMaxUpdater(instance, updaterName).max();
        long maxAsRecorded = findMax(maxArray);
        assertEquals(maxAsRecorded, maxFromHZ);
    }

    private long findMax(long[] maxArray) {
        long max = Long.MIN_VALUE;
        for (long value : maxArray) max = Math.max(max, value);
        return max;
    }

    private ILongMaxUpdater getLongMaxUpdater(HazelcastInstance instance, String name) {
        ILongMaxUpdater update =  instance.getDistributedObject(LongMaxUpdaterService.SERVICE_NAME, name);
        return update;
    }

    private Config getConfig() {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig().setClassName(LongMaxUpdaterService.class.getName()).setName(LongMaxUpdaterService.SERVICE_NAME).setEnabled(true);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

}
