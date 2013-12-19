package info.jerrinot.hazelcast.longmaxupdater;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import info.jerrinot.hazelcast.longmaxupdater.spi.LongMaxUpdaterService;
import info.jerrinot.hazelcast.longmaxupdater.spi.proxy.LongMaxUpdaterInvocationHandler;

import java.lang.reflect.Proxy;

public class HazelcastExtender {

    public static Config createConfig(Config config) {
        ServiceConfig serviceConfig = new ServiceConfig().setClassName(LongMaxUpdaterService.class.getName()).setName(LongMaxUpdaterService.SERVICE_NAME).setEnabled(true);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

    public static Config createConfig() {
        Config config = new Config();
        return HazelcastExtender.createConfig(config);
    }

    public static ExtendedHazelcastInstance extend(HazelcastInstance instance) {
        if (instance instanceof ExtendedHazelcastInstance) {
            return (ExtendedHazelcastInstance) instance;
        }

        return (ExtendedHazelcastInstance) Proxy.newProxyInstance(LongMaxUpdaterInvocationHandler.class.getClassLoader(),
                new Class[]{HazelcastInstance.class, ExtendedHazelcastInstance.class},
                new LongMaxUpdaterInvocationHandler(instance));
    }
}
