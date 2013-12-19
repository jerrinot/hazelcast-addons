package info.jerrinot.hazelcast.longmaxupdater.spi.proxy;

import com.hazelcast.core.HazelcastInstance;
import info.jerrinot.hazelcast.longmaxupdater.spi.LongMaxUpdaterService;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class LongMaxUpdaterInvocationHandler implements InvocationHandler {

    private HazelcastInstance instance;

    public LongMaxUpdaterInvocationHandler(HazelcastInstance instance) {
        this.instance = instance;
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (method.getName ().equals("getLongMaxUpdater") && parameterTypes.length == 1 && parameterTypes[0].equals(String.class)) {
            String name = (String) args[0];
            return instance.getDistributedObject(LongMaxUpdaterService.SERVICE_NAME, name);
        }
        return method.invoke(instance, args);
    }
}
