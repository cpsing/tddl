package com.taobao.tddl.monitor.stat;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 带内存汇总功能的日志输出工具的另一种实现, 使用 SoftReference/WeakReferece 解决 BufferedLogWriter
 * OldGen 内存占用和 FullGC的问题。 <br />
 * 这个工具的风险是 SoftReference 的清理时机完全由 JVM 确定, 在大部分情况下都不会回收 SoftReference 除非内存不足,
 * 这样会造成不必要的内存占用。 <br />
 * 如果使用 WeakReference 的风险是频繁的 minor GC 会回收统计记录, 造成大量统计记录的丢失。 <br />
 * 因此建议的使用方法是：
 * 
 * <pre>
 * // 创建 counter 对象, 并且保存在内存
 * LogCounter counter = SoftRefLogWriter.getCounter(
 *     new Object[] { key1, key2, key3, ... });  // 统计的目标 
 * 
 * // 创建 counter 对象, 并且保存在内存
 * LogCounter counter = SoftRefLogWriter.getCounter(
 *     new Object[] { key1, key2, key3, ... },   // 统计的目标 
 *     new Object[] { obj1, obj2, obj3, ... });  // 附加的对象
 * 
 * counter.stat(count, value);  // 输出统计值
 * </pre>
 * 
 * @author changyuan.lh
 */
public class SoftRefLogWriter extends AbstractStatLogWriter {

    protected static final Logger logger        = LoggerFactory.getLogger(SoftRefLogWriter.class);
    protected volatile int        flushInterval = 300;                                            // 单位秒,默认5分钟全量刷出一次
    protected volatile boolean    softRef       = true;

    protected static final class WeakRefLogCounter extends WeakReference<LogCounter> {

        final LogKey logKey;

        public WeakRefLogCounter(LogKey logKey, LogCounter counter, ReferenceQueue<LogCounter> queue){
            super(counter, queue);
            this.logKey = logKey;
        }
    }

    protected static final class SoftRefLogCounter extends SoftReference<LogCounter> {

        final LogKey logKey;

        public SoftRefLogCounter(LogKey logKey, LogCounter counter, ReferenceQueue<LogCounter> queue){
            super(counter, queue);
            this.logKey = logKey;
        }
    }

    protected volatile ConcurrentHashMap<LogKey, Reference<LogCounter>> map;

    /**
     * Reference queue for cleared WeakEntries
     */
    private final ReferenceQueue<LogCounter>                            queue = new ReferenceQueue<LogCounter>();

    protected final StatLogWriter                                       nestLog;

    public SoftRefLogWriter(boolean softRef, int flushInterval, StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, Reference<LogCounter>>( // NL
        1024,
            0.75f,
            32);
        this.flushInterval = flushInterval;
        this.softRef = softRef;
        this.nestLog = nestLog;
        schdeuleFlush();
    }

    public SoftRefLogWriter(boolean softRef, StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, Reference<LogCounter>>( // NL
        1024,
            0.75f,
            32);
        this.softRef = softRef;
        this.nestLog = nestLog;
        schdeuleFlush();
    }

    public SoftRefLogWriter(StatLogWriter nestLog){
        this.map = new ConcurrentHashMap<LogKey, Reference<LogCounter>>( // NL
        1024,
            0.75f,
            32);
        this.nestLog = nestLog;
        schdeuleFlush();
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

    public boolean isSoftRef() {
        return softRef;
    }

    public void setSoftRef(boolean softRef) {
        this.softRef = softRef;
    }

    /**
     * 创建记录对象的 SoftReference/WeakReference.
     */
    protected final Reference<LogCounter> createLogRef(LogKey logKey, LogCounter counter) {
        return softRef ? new SoftRefLogCounter(logKey, counter, queue) : new WeakRefLogCounter(logKey, counter, queue);
    }

    /**
     * 清理已经回收的对象占据的 map entry.
     */
    protected final void expungeLogRef() {
        final long expungeMillis = System.currentTimeMillis();
        int count = 0;
        Reference<? extends LogCounter> entry;
        while ((entry = queue.poll()) != null) {
            if (entry instanceof SoftRefLogCounter) {
                map.remove(((SoftRefLogCounter) entry).logKey);
                count++;
            } else if (entry instanceof WeakRefLogCounter) {
                map.remove(((WeakRefLogCounter) entry).logKey);
                count++;
            }
        }
        if (count > 0 && logger.isDebugEnabled()) {
            logger.debug("expungeLogRef: " + count + " logs in " + (System.currentTimeMillis() - expungeMillis)
                         + " milliseconds.");
        }
    }

    /**
     * 创建一个记录对象, 或者返回缓存中已有的记录。
     */
    public LogCounter getCounter(Object[] keys, Object[] fields) {
        ConcurrentHashMap<LogKey, Reference<LogCounter>> map = this.map;
        LogKey logKey = new LogKey(keys);
        LogCounter counter;
        for (;;) {
            Reference<LogCounter> entry = map.get(logKey);
            if (entry == null) {
                LogCounter newCounter = new LogCounter(logKey, (fields == null) ? keys : fields);
                entry = map.putIfAbsent(logKey, createLogRef(logKey, newCounter));
                if (entry == null) {
                    expungeLogRef();
                    return newCounter;
                }
            }
            counter = entry.get();
            if (counter != null) {
                return counter;
            }
            map.remove(logKey);
        }
    }

    /**
     * 在内存中汇总统计信息。
     */
    public void write(Object[] keys, Object[] fields, long... values) {
        if (values.length != 2) {
            // XXX: 这里限制 BufferedLogWriter 只接受 count + value 的输入
            throw new IllegalArgumentException("Only support 2 values!");
        }
        ConcurrentHashMap<LogKey, Reference<LogCounter>> map = this.map;
        LogKey logKey = new LogKey(keys);
        LogCounter counter;
        for (;;) {
            Reference<LogCounter> entry = map.get(logKey);
            if (entry == null) {
                LogCounter newCounter = new LogCounter(logKey, (fields == null) ? keys : fields);
                newCounter.stat(values[0], values[1]);
                entry = map.putIfAbsent(logKey, createLogRef(logKey, newCounter));
                if (entry == null) {
                    expungeLogRef();
                    return;
                }
            }
            counter = entry.get();
            if (counter != null) {
                counter.stat(values[0], values[1]);
                return;
            }
            map.remove(logKey);
        }
    }

    protected volatile TimerTask flushTask = null;

    protected final Lock         flushLock = new ReentrantLock();

    protected volatile boolean   flushing  = false;

    public boolean flush() {
        if (!flushing && flushLock.tryLock()) {
            try {
                flushing = true;
                flushExecutor.execute(new Runnable() {

                    public void run() {
                        try {
                            flushAll();
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
                flush();
            }
        };
        if (cancelTask != null) {
            cancelTask.cancel();
        }
        final long flushPriod = flushInterval * 1000;
        flushTimer.scheduleAtFixedRate(flushTask, flushPriod, flushPriod);
    }

    /**
     * 刷出所有的日志统计信息。
     */
    protected void flushAll() {
        final long flushMillis = System.currentTimeMillis();
        // 清理已经回收的对象
        expungeLogRef();
        // XXX: 输出的日志按 Key 进行排序 -- 先取消
        // TreeMap<LogKey, Reference<LogCounter>> map = new TreeMap<LogKey,
        // SoftReference<LogCounter>>(map);
        ConcurrentHashMap<LogKey, Reference<LogCounter>> map = this.map;
        int count = 0;
        for (Entry<LogKey, Reference<LogCounter>> entry : map.entrySet()) {
            LogCounter counter = entry.getValue().get();
            if (counter != null && counter.getCount() > 0) {
                LogKey logKey = entry.getKey();
                nestLog.write(logKey.getKeys(), counter.getFields(), counter.getValues());
                counter.clear();
                count++;
            }
        }
        if (count > 0 && logger.isDebugEnabled()) {
            logger.debug("flushAll: " + count + " logs in " + (System.currentTimeMillis() - flushMillis)
                         + " milliseconds.");
        }
    }
}
