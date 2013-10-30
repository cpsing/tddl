package com.taobao.tddl.monitor.stat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象类 -- 用来在 BufferedLogWriter/SoftRefLogWriter 之间共享一些对象。
 * 
 * @author changyuan.lh
 */
public abstract class AbstractStatLogWriter extends StatLogWriter {

    protected static ExecutorService flushExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {

                                                       // changyuan.lh: 给线程命名,
                                                       // 并且设置成后台运行
                                                       public Thread newThread(Runnable r) {
                                                           Thread thd = new Thread(r,
                                                               "BufferedStatLogWriter-Flush-Executor");
                                                           thd.setDaemon(true);
                                                           return thd;
                                                       }
                                                   });

    protected static Timer           flushTimer    = new Timer( // NL
                                                   "BufferedStatLogWriter-Flush-Timer",
                                                       true);

    public static final class UniversalComparator implements Comparator<Object> {

        public static final UniversalComparator INSTANCE = new UniversalComparator();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public int compare(Object o1, Object o2) {
            if (o1 == o2) return 0;
            if (o1 == null) return -1;
            if (o2 == null) return 1;
            if (o1.getClass() == o2.getClass()) {
                if (o1 instanceof Comparable) return ((Comparable) o1).compareTo(o2);
            }
            return System.identityHashCode(o1) - System.identityHashCode(o2);
        }
    }

    public static final class ArrayComparator implements Comparator<Object[]> {

        public static final ArrayComparator INSTANCE = new ArrayComparator();

        public int compare(Object[] a1, Object[] a2) {
            if (a1 == a2) return 0;
            if (a1 == null) return -1;
            if (a2 == null) return 1;
            Comparator<Object> comparator = UniversalComparator.INSTANCE;
            final int min = (a1.length < a2.length) ? a1.length : a2.length;
            for (int i = 0; i < min; i++) {
                final int cmp = comparator.compare(a1[i], a2[i]);
                if (cmp != 0) return cmp;
            }
            return a1.length - a2.length;
        }
    }

    public static final class LogKey implements Comparable<LogKey> {

        private final Object[] keys;
        private final int      hashCode;

        public LogKey(Object[] keys){
            this.hashCode = Arrays.hashCode(keys);
            this.keys = keys;
        }

        public Object[] getKeys() {
            return keys;
        }

        public int compareTo(LogKey obj) {
            if (this == obj) return 0;
            if (obj == null) return 1;
            return ArrayComparator.INSTANCE.compare(keys, obj.keys);
        }

        public int hashCode() {
            return hashCode;
        }

        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            LogKey other = (LogKey) obj;
            return Arrays.equals(keys, other.keys);
        }
    }

    public static class LogCounter implements Comparable<LogCounter> {

        private final LogKey     logKey;
        private final Object[]   fields;
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong value = new AtomicLong();
        private final AtomicLong min   = new AtomicLong(Long.MAX_VALUE); // value最小值
        private final AtomicLong max   = new AtomicLong(Long.MIN_VALUE); // value最大值

        public LogCounter(LogKey logKey, Object[] fields){
            this.logKey = logKey;
            this.fields = fields;
        }

        public final LogKey getLogKey() {
            return logKey;
        }

        public final Object[] getFields() {
            return this.fields;
        }

        public int compareTo(LogCounter obj) {
            if (this == obj) return 0;
            if (obj == null) return 1;
            long c1 = count.get();
            long c2 = obj.count.get();
            return (c1 < c2 ? -1 : (c1 == c2 ? 0 : 1));
        }

        public void stat(long c, long v) {
            this.count.addAndGet(c);
            this.value.addAndGet(v);
            long vmin = min.get();
            while (v < vmin && !min.compareAndSet(vmin, v)) {
                vmin = min.get(); // 有可能已经被其他线程设置了一个次小的，所以继续判断
            }
            long vmax = max.get();
            while (v > vmax && max.compareAndSet(vmax, v)) {
                vmax = max.get(); // 有可能已经被其他线程设置了一个次大的，所以继续判断
            }
        }

        public void clear() {
            this.count.lazySet(0L);
            this.value.lazySet(0L);
            this.min.lazySet(Long.MAX_VALUE);
            this.max.lazySet(Long.MIN_VALUE);
        }

        public final long getCount() {
            return this.count.get();
        }

        public final long getValue() {
            // XXX: 保持兼容, 输出总数而不是平均数
            return this.value.get();
        }

        public final long getMin() {
            return this.min.get();
        }

        public final long getMax() {
            return this.max.get();
        }

        public long[] getValues() {
            return new long[] { this.count.get(), this.value.get(), this.min.get(), this.max.get() };
        }
    }
}
