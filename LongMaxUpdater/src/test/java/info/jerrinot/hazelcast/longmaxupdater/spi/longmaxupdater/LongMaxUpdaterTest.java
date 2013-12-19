package info.jerrinot.hazelcast.longmaxupdater.spi.longmaxupdater;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import info.jerrinot.hazelcast.longmaxupdater.ExtendedHazelcastInstance;
import info.jerrinot.hazelcast.longmaxupdater.HazelcastExtender;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import info.jerrinot.hazelcast.longmaxupdater.ILongMaxUpdater;
import info.jerrinot.hazelcast.longmaxupdater.spi.LongMaxUpdaterService;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class LongMaxUpdaterTest extends HazelcastTestSupport {

    @Test
    public void testMaxThenReset() {
        final ExtendedHazelcastInstance instance = HazelcastExtender.extend(createHazelcastInstanceFactory(1).newInstances(getConfig())[0]);
        ILongMaxUpdater updater = instance.getLongMaxUpdater("updater");

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
        final ExtendedHazelcastInstance instance = HazelcastExtender.extend(createHazelcastInstanceFactory(3).newInstances(getConfig())[0]);

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
                    ILongMaxUpdater updater = instance.getLongMaxUpdater(updaterName);
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

        long maxFromHZ = instance.getLongMaxUpdater(updaterName).max();
        long maxAsRecorded = findMax(maxArray);
        assertEquals(maxAsRecorded, maxFromHZ);
    }


    @Test
    public void testMigrations() {
        int noOfUpdaters = 500;
        int noOfInstances = 5;
        int maximum = 50;
        HazelcastInstance[] instances = createHazelcastInstanceFactory(noOfInstances).newInstances(getConfig());

        ExtendedHazelcastInstance instance = HazelcastExtender.extend(instances[0]);
        //create updaters
        ILongMaxUpdater[] updaters = new ILongMaxUpdater[noOfUpdaters];
        for (int i = 0; i < noOfUpdaters; i++) {
            ILongMaxUpdater updater = instance.getLongMaxUpdater("updater" + i);
            updaters[i] = updater;
        }

        //call 0..99 on each updater
        for (int i = 0; i <= maximum; i++) {
            for (int j = 0; j < noOfUpdaters; j++) {
                updaters[j].update(i);
            }
        }

        //this should be ignored by the updates
        for (int j = 0; j < noOfUpdaters; j++) {
            updaters[j].update(maximum - 1);
        }

        //kill or shutdown all, but last instance
        for (int i = 0; i < noOfInstances-1; i++) {
            if (i == 0)  {
                TestUtil.terminateInstance(instances[0]); //always kill the 1st instance
            } else {
                instances[i].shutdown();
            }
        }
        instance = HazelcastExtender.extend(instances[noOfInstances-1]); //get the last (still alive) instance

        for (int i = 0; i < noOfUpdaters; i++) {
            long max = instance.getLongMaxUpdater("updater" + i).max();
            assertEquals("Instance no "+i+" has a wrong value", maximum, max);
        }


    }

    private long findMax(long[] maxArray) {
        long max = Long.MIN_VALUE;
        for (long value : maxArray) max = Math.max(max, value);
        return max;
    }

    private Config getConfig() {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig().setClassName(LongMaxUpdaterService.class.getName()).setName(LongMaxUpdaterService.SERVICE_NAME).setEnabled(true);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

}
