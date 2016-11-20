package org.everythingjboss.jdg;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.CacheCollectors;

public class SimpleDistributedStreams {

    private static final Logger logger = LogManager.getLogger(SimpleDistributedStreams.class);

    public static void main(String[] args) throws InterruptedException, IOException {

        CountDownLatch cdl = new CountDownLatch(1);
        EmbeddedCacheManager cacheManager = new DefaultCacheManager("infinispan.xml");
        cacheManager.start();

        Cache<String, String> cache = cacheManager.getCache("streams");
        cache.addListener(new ClusterListener());

        if (cacheManager.isCoordinator()) {
            logger.info("*** This is the coordinator instance ***");
            cacheManager.addListener(new ClusterListener(cdl));
            cdl.await();
            Thread.sleep(5000);
        }

        if (cacheManager.isCoordinator()) {
            cache.put("1", "He is great");
            cache.put("2", "I am great");
            cache.put("3", "The world is great");

            // Perform Map/Reduce of the cache entries
            Map<String, Long> wordCountMap = cache.entrySet().parallelStream()
                    .map((Serializable & Function<Map.Entry<String, String>, String[]>) e -> e.getValue().split("\\s"))
                    .flatMap((Serializable & Function<String[], Stream<String>>) Arrays::stream)
                    .collect(CacheCollectors.serializableCollector(
                            () -> Collectors.groupingBy(Function.identity(), Collectors.counting())));

            logger.info("The word count map looks like :\n"+wordCountMap);

            DistributedExecutorService des = new DefaultExecutorService(cache);
            DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new CacheManagerShutdownTask());

            DistributedTask<Boolean> task = taskBuilder.build();

            List<Address> addresses = cache.getCacheManager().getMembers();

            addresses.stream().forEach(address -> {
                if (!cacheManager.getAddress().equals(address)) {
                    des.submit(address, task);
                }
            });

            cacheManager.stop();
        }
    }
}

class CacheManagerShutdownTask implements DistributedCallable<String, String, Boolean>, Serializable {
    
    private static final Logger logger = LogManager.getLogger(CacheManagerShutdownTask.class);
    private static final long serialVersionUID = 1L;
    private Cache<String, String> cache;

    @Override
    public Boolean call() throws Exception {
        logger.info("CacheManager STOP command received");
        cache.getCacheManager().stop();
        return true;
    }

    @Override
    public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
        this.cache = cache;
    }
}
