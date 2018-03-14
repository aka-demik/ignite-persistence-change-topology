package ru.aka.ignite.topology.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

import static java.util.Optional.ofNullable;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Ignite ignite = createNode();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            IgniteCache cache = null;
            String cmd = "";
            do {
                switch (cmd) {
                    case "activate":
                        LOG.info("activate cluster");
                        if (!ignite.active()) {
                            ignite.active(true);
                        }
                        cache = ignite.cache("test");
                        break;

                    case "fill":
                        fillCache(ignite);
                        break;

                    case "serve":
                        ignite.services().deployNodeSingleton("testService", new JustService());
                        break;

                    default:
                        LOG.info("XX==================================================XX");
                        if (cache != null) {
                            LOG.info("cache size: {}", cache.sizeLong(CachePeekMode.PRIMARY));
                            LOG.info("local cache size: {}", cache.localSize(CachePeekMode.PRIMARY));
                        }
                        LOG.info("commands: activate, fill, serve, q.");
                        break;
                }
                cmd = reader.readLine();
            } while (!"q".equalsIgnoreCase(cmd));

            LOG.info("application stopping");

        } catch (IOException e) {
            LOG.error("", e);
        } finally {
            Ignition.stop(ignite.name(), false);
        }
    }

    private static Ignite createNode() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        String iHosts = ofNullable(System.getenv("I_HOSTS")).orElse("192.168.174.210,192.168.152.123");
        spi.setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(iHosts.split(","))));

        CacheConfiguration cacheConfiguration = new CacheConfiguration("test");
        cacheConfiguration
                .setBackups(1)
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceMode(CacheRebalanceMode.SYNC);

        MemoryConfiguration memoryConfig = new MemoryConfiguration();
        memoryConfig.setDefaultMemoryPolicySize(5L * 1024 * 1024 * 1024);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi()
                .setMessageQueueLimit(1024);

        PersistentStoreConfiguration storeConfiguration = new PersistentStoreConfiguration();
        storeConfiguration
                .setWalMode(WALMode.BACKGROUND)
                .setCheckpointingFrequency(10_000);

        cfg
                .setDiscoverySpi(spi)
                .setCommunicationSpi(commSpi)
                .setMemoryConfiguration(memoryConfig)
                .setCacheConfiguration(cacheConfiguration)
                .setPersistentStoreConfiguration(storeConfiguration)
                .setIgniteHome(ofNullable(System.getenv("I_HOME")).orElse("/opt/ignite-persistence-change-topology/"));

        return Ignition.start(cfg);
    }

    private static void fillCache(Ignite ignite) {
        Random r = new Random();
        long tm = System.currentTimeMillis();
        try (IgniteDataStreamer<Long, long[]> test = ignite.dataStreamer("test")) {
            LOG.info("Start cache filling");
            for (long i = 0; i < JustServiceMode.CACHE_MAX_SIZE; i++) {
                test.addData(i, r.longs(512).toArray());
            }
        }
        LOG.info("Cache filling done. count = {}, time = {}", JustServiceMode.CACHE_MAX_SIZE, System.currentTimeMillis() - tm);
    }
}
