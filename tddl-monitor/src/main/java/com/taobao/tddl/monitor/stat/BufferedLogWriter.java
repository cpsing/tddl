package com.taobao.tddl.monitor.stat;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 带内存汇总功能的日志输出工具。解决高 TPS 场景统计日志量太大的问题。 <br />
 * 工具的功能是针对 Key 相同的统计记录, 汇总计算出 sum/min/max, 再定时刷出到日志。 <br />
 * 目前遗留的问题是 BufferedLogWriter 在 flushAll 丢弃缓存的日志对象, 因为这些对象生存期长已经进入 Old Gen,
 * 长时间运行会造成 Old Gen 爆掉触发 Full GC. <br />
 * 使用方法是：
 * 
 * <pre>
 * BufferStatLogWriter.write(
 *     new Object[] { key1, key2, key3, ... },  // 统计的目标 
 *     count, value);                           // 统计值
 * 
 * BufferStatLogWriter.write(
 *     new Object[] { key1, key2, key3, ... },  // 统计的目标 
 *     new Object[] { obj1, obj2, obj3, ... },  // 附加的对象
 *     count, value);                           // 统计值
 * </pre>
 * 
 * @author changyuan.lh
 */
public class BufferedLogWriter extends AbstractStatLogWriter {

    protected static final Logger                            logger        = LoggerFactory.getLogger(BufferedLogWriter.class);
    public volatile int                                      flushInterval = 300;                                             // 单位秒,默认5分钟全量刷出一次
    protected volatile int                                   minKeySize    = 1024;
    public volatile int                                      maxKeySize    = 65536;

    protected volatile ConcurrentHashMap<LogKey, LogCounter> map;

    protected final StatLogWriter                            nestLog;

    public BufferedLogWriter(int flushInterval, int minKeySize, int maxKeySize, StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, LogCounter>( // NL
        minKeySize,
            0.75f,
            32);
        this.flushInterval = flushInterval;
        this.minKeySize = minKeySize;
        this.maxKeySize = maxKeySize;
        this.nestLog = nestLog;
        schdeuleFlush();
    }

    public BufferedLogWriter(int minKeySize, int maxKeySize, StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, LogCounter>( // NL
        minKeySize,
            0.75f,
            32);
        this.minKeySize = minKeySize;
        this.maxKeySize = maxKeySize;
        this.nestLog = nestLog;
        schdeuleFlush();
    }

    public BufferedLogWriter(StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, LogCounter>( // NL
        minKeySize,
            0.75f,
            32);
        this.nestLog = nestLog;
        schdeuleFlush();
    }

    public void setMinKeySize(int minKeySize) {
        this.minKeySize = minKeySize;
    }

    public int getMinKeySize() {
        return minKeySize;
    }

    public void setMaxKeySize(int maxKeySize) {
        this.maxKeySize = maxKeySize;
    }

    public int getMaxKeySize() {
        return maxKeySize;
    }

    public void setFlushInterval(int flushInterval) {
        if (this.flushInterval != flushInterval) {
            this.flushInterval = flushInterval;
            schdeuleFlush();
        }
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    /**
     * 在内存中汇总统计信息, 如果总量超过 maxKeySize 限制, 异步执行 flushLRU().
     */
    public void write(Object[] keys, Object[] fields, long... values) {
        if (values.length != 2) {
            // XXX: 这里限制 BufferedLogWriter 只接受 count + value 的输入
            throw new IllegalArgumentException("Only support 2 values!");
        }
        ConcurrentHashMap<LogKey, LogCounter> map = this.map;
        LogKey logKey = new LogKey(keys);
        LogCounter counter = map.get(logKey);
        if (counter == null) {
            LogCounter newCounter = new LogCounter(logKey, (fields == null) ? keys : fields);
            newCounter.stat(values[0], values[1]);
            counter = map.putIfAbsent(logKey, newCounter);
            if (counter == null) {
                insureMaxSize();
                return;
            }
        }
        counter.stat(values[0], values[1]);
    }

    protected void insureMaxSize() {
        if (map.size() > maxKeySize) {
            flush(false);
        }
    }

    protected volatile TimerTask flushTask = null;

    protected final Lock         flushLock = new ReentrantLock();

    protected volatile boolean   flushing  = false;

    public boolean flush(final boolean flushAll) {
        if (!flushing && flushLock.tryLock()) {
            try {
                flushing = true;
                flushExecutor.execute(new Runnable() {

                    public void run() {
                        try {
                            if (flushAll) {
                                flushAll();
                            } else {
                                flushLRU();
                            }
                        } finally {
                            flushing = false;
                        }
                    }
                });
            } finally {
                flushLock.unlock();
            }
            return true;
        }
        return false;
    }

    private final synchronized void schdeuleFlush() {
        TimerTask cancelTask = this.flushTask;
        this.flushTask = new TimerTask() {

            public void run() {
                // XXX: 定时器的执行应当耗时非常短
                flush(true);
            }
        };
        if (cancelTask != null) {
            cancelTask.cancel();
        }
        final long flushPriod = flushInterval * 1000;
        flushTimer.scheduleAtFixedRate(flushTask, flushPriod, flushPriod);
    }

    private final int flushLog(Map<LogKey, LogCounter> logs) {
        int count = 0;
        for (Entry<LogKey, LogCounter> entry : logs.entrySet()) {
            LogCounter counter = entry.getValue();
            nestLog.write(entry.getKey().getKeys(), counter.getFields(), counter.getValues());
            count++;
        }
        return count;
    }

    private final void flushLog(LogKey logKey, LogCounter counter) {
        nestLog.write(logKey.getKeys(), counter.getFields(), counter.getValues());
    }

    /**
     * 刷出所有的日志统计信息。
     */
    protected void flushAll() {
        final long flushMillis = System.currentTimeMillis();
        final int initKeySize = Math.max(minKeySize, (int) (map.size() / 0.75f));
        Map<LogKey, LogCounter> map = this.map;
        this.map = new ConcurrentHashMap<LogKey, LogCounter>( // NL
        initKeySize,
            0.75f,
            32);
        // 等待正在添加记录的线程执行完毕
        LockSupport.parkNanos(5000);
        // XXX: 输出的日志按 Key 进行排序 -- 先取消
        // map = new TreeMap<LogKey, LogCounter>(map);
        int count = flushLog(map);
        if (count > 0 && logger.isDebugEnabled()) {
            logger.debug("flushAll: " + map.size() + " logs in " + (System.currentTimeMillis() - flushMillis)
                         + " milliseconds.");
        }
    }

    /**
     * 刷出统计次数最少的日志信息, 只保留 maxKeySize 总量的 2/3.
     */
    protected void flushLRU() {
        final long flushMillis = System.currentTimeMillis();
        // XXX: 输出的日志按 Key 进行排序 -- 先取消
        // Map<LogKey, LogCounter> flushLogs = new TreeMap<LogKey,
        // LogCounter>();
        Map<LogKey, LogCounter> map = this.map;
        int keep = maxKeySize * 2 / 3; // 保留 2/3
        int flush = map.size() - keep; // 这时 size 可能已经增长
        int count = 0;
        // changyuan.lh: 首先清理 count = 1 的记录, 不需要分配内存速度比较快
        for (Entry<LogKey, LogCounter> entry : map.entrySet()) {
            if (flush <= 0) break;
            LogKey logKey = entry.getKey();
            LogCounter counter = entry.getValue();
            if (counter.getCount() < 2) {
                LogCounter removed = map.remove(logKey);
                if (removed != null) {
                    // flushLogs.put(logKey, counter);
                    flushLog(logKey, removed);
                    flush--;
                    count++;
                }
            }
        }
        // changyuan.lh: 然后 LRU 清理全部的记录, 需要进行排序, 内存占用比较高
        flush = map.size() - keep; // 这时 size 可能已经增长
        if (flush > 0) {
            Object[] counters = map.values().toArray();
            Arrays.sort(counters);
            for (int i = 0; i < Math.min(flush, counters.length); i++) {
                LogCounter counter = (LogCounter) counters[i];
                LogKey logKey = counter.getLogKey();
                LogCounter removed = map.remove(logKey);
                if (removed != null) {
                    // flushLogs.put(logKey, removed);
                    flushLog(logKey, counter);
                    count++;
                }
            }
        }
        // flushLog(flushLogs);
        if (count > 0 && logger.isDebugEnabled()) {
            logger.debug("flushLRU: " + count + " logs in " + (System.currentTimeMillis() - flushMillis)
                         + " milliseconds.");
        }
    }
}
