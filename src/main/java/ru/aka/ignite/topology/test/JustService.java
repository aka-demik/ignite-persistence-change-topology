package ru.aka.ignite.topology.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JustService implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(JustService.class);

    @IgniteInstanceResource
    private transient Ignite ignite;
    private transient IgniteCache<Long, long[]> cache;
    private transient AtomicBoolean stop;
    private transient Thread threadRead;
    private transient Thread threadWrite;
    private transient Thread threadStat;
    private transient ArrayList<Long> reads;
    private transient ArrayList<Long> writes;

    @Override
    public void cancel(ServiceContext ctx) {
        LOG.info("cancel");
        stop.set(true);

        joinThread(threadRead);
        threadRead = null;

        joinThread(threadWrite);
        threadWrite = null;

        joinThread(threadStat);
        threadStat = null;
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        LOG.info("init");
        cache = ignite.cache("test");
        stop = new AtomicBoolean();
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        LOG.info("execute");
        reads = new ArrayList<>(4 * 1024);
        writes = new ArrayList<>(4 * 1024);
        stop.set(false);

        threadRead = new Thread(this::runRead);
        threadWrite = new Thread(this::runWrite);
        threadStat = new Thread(this::runStat);
        threadRead.start();
        threadWrite.start();
        threadStat.start();
    }

    private void joinThread(Thread t) {
        if (t != null) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runRead() {
        LOG.info("runRead");

        Random r = new Random();

        try {
            int n = 0;
            while (!stop.get() && !Thread.interrupted()) {
                long key = r.nextInt(JustServiceMode.CACHE_MAX_SIZE);

                long t1 = System.nanoTime();
                long sum = 1;
                long[] longs = cache.get(key);
                long t2 = System.nanoTime();

                if (longs == null) {
                    longs = r.longs(512).toArray();
                }
                for (long l : longs) {
                    sum += l;
                }

                if (sum == 0) {
                    LOG.error("Just use results...");
                }

                synchronized (reads) {
                    reads.add(t2 - t1);
                }

                n += 1;
                if (n > JustServiceMode.PAUSE_MULT) {
                    n = 0;
                    Thread.sleep(5);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("", e);
            Thread.currentThread().interrupt();
        }
    }

    private void runWrite() {
        LOG.info("runWrite");

        int wps;
        int n = 0;
        Random r = new Random();

        try {
            while (!stop.get() && !Thread.interrupted()) {
                long[] longs = r.longs(512).toArray();

                long t1 = System.nanoTime();
                cache.put((long) r.nextInt(JustServiceMode.CACHE_MAX_SIZE), longs);
                long t2 = System.nanoTime();

                synchronized (writes) {
                    writes.add(t2 - t1);
                    wps = writes.size();
                }

                if (wps > 1700) { // Soft limit to 1700 writes per second
                    Thread.sleep(5);
                }

                n += 1;
                if (n > JustServiceMode.PAUSE_MULT) {
                    n = 0;
                    Thread.sleep(5);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("", e);
            Thread.currentThread().interrupt();
        }
    }

    private void runStat() {
        LOG.info("runStat");
        Path path = Paths.get(ignite.configuration().getIgniteHome(), "stat.csv");

        try (BufferedWriter stat = Files.newBufferedWriter(path, StandardOpenOption.CREATE)) {
            stat.write("time;RPS;r-min;r-avg;r-max;WPS;w-min;w-avg;w-max");
            stat.newLine();

            while (!stop.get() && !Thread.interrupted()) {
                Thread.sleep(1000);

                LongSummaryStatistics readsStat;
                LongSummaryStatistics writesStat;

                synchronized (reads) {
                    readsStat = reads.stream().mapToLong(value -> value).summaryStatistics();
                    reads.clear();
                }

                synchronized (writes) {
                    writesStat = writes.stream().mapToLong(value -> value).summaryStatistics();
                    writes.clear();
                }

                String s = String.format("RPS: %5d,%3d,%6d|WPS:%5d,%3d,%6d",
                        readsStat.getCount(),
                        (readsStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis((long) readsStat.getAverage()) : 0,
                        (readsStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(readsStat.getMax()) : 0,
                        writesStat.getCount(),
                        (writesStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis((long) writesStat.getAverage()) : 0,
                        (writesStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(writesStat.getMax()) : 0);
                LOG.info(s);
                s = String.format("%s;%d;%d;%d;%d;%d;%d;%d;%d",
                        LocalTime.now().toString(),
                        readsStat.getCount(),
                        (readsStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(readsStat.getMin()) : 0,
                        (readsStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis((long) readsStat.getAverage()) : 0,
                        (readsStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(readsStat.getMax()) : 0,
                        writesStat.getCount(),
                        (writesStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(writesStat.getMin()) : 0,
                        (writesStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis((long) writesStat.getAverage()) : 0,
                        (writesStat.getCount() != 0) ? TimeUnit.NANOSECONDS.toMillis(writesStat.getMax()) : 0);
                stat.write(s);
                stat.newLine();
                stat.flush();
            }
        } catch (InterruptedException | IOException e) {
            LOG.error("", e);
            Thread.currentThread().interrupt();
        }
    }
}
