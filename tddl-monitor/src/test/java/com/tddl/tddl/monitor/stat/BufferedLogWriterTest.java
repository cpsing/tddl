package com.tddl.tddl.monitor.stat;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Ignore;

import com.taobao.tddl.monitor.stat.BufferedLogWriter;
import com.taobao.tddl.monitor.stat.LoggerLogWriter;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

@Ignore
public class BufferedLogWriterTest {

    private static final Logger            TDDL_Test_Statistic_LOG = LoggerFactory.getLogger("TDDL_Test_Statistic_LOG");

    private static final BufferedLogWriter logger                  = new BufferedLogWriter(1024,
                                                                       131072,
                                                                       new LoggerLogWriter(", ",
                                                                           TDDL_Test_Statistic_LOG));

    private static void perfTest(final int concurrent, final int seconds) throws InterruptedException {
        final Random rand = new Random();
        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicLong counter = new AtomicLong();
        final AtomicLong latency = new AtomicLong();
        Thread[] threads = new Thread[concurrent];
        for (int j = 0; j < concurrent; j++) {
            threads[j] = new Thread() {

                @Override
                public void run() {
                    while (!exit.get()) {
                        String db = "db_" + rand.nextInt(256);
                        String key = "key_" + (rand.nextInt(64));
                        String status = "status_" + (rand.nextInt(8));
                        final long delay = 1000 + Math.abs(rand.nextLong()) % 2000;
                        final long nanos = System.nanoTime();
                        logger.stat(db, key, status, delay);
                        latency.addAndGet(System.nanoTime() - nanos);
                        counter.incrementAndGet();
                        LockSupport.parkNanos(1000);
                    }
                }
            };
            threads[j].start();
        }

        Thread.sleep(seconds * 1000);
        System.out.println("concurrent: " + concurrent + ", seconds: " + seconds + ", number: " + counter.get()
                           + ", RT: " + (latency.get() / counter.get()) + ", TPS: "
                           + ((long) (counter.get() * 100 / seconds)) / 100);

        exit.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        perfTest(10, 400);
        perfTest(20, 400);
        perfTest(40, 400);
        perfTest(80, 400);
        perfTest(100, 400);
        perfTest(60, 400);
        perfTest(10, 400);
    }
}
