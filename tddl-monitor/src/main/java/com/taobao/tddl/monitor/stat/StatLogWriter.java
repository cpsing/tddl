package com.taobao.tddl.monitor.stat;

/**
 * 统计信息的日志输出接口。 <br />
 * 工具的功能是对相同目标的统计数据合并输出, 计算出 min/max/avg, 定时刷出到日志。 <br />
 * 
 * <pre>
 * StatLogWriter.write(
 *     new Object[] { key1, key2, key3, ... },  // 统计的目标
 *     number1, number2, ...);                  // 统计值
 * 
 * StatLogWriter.write(
 *     new Object[] { key1, key2, key3, ... },  // 统计的目标
 *     new Object[] { obj1, obj2, obj3, ... },  // 实际的输出项
 *     number1, number2, ...);                  // 统计值
 * </pre>
 * 
 * @author changyuan.lh
 */
public abstract class StatLogWriter {

    public final void write(Object[] keys, long... values) {
        write(keys, keys, values);
    }

    /**
     * 统计信息分成三部分：汇总目标/key, 统计目标信息/fields, 数据/values. 首先组件会根据汇总目标/key
     * 在内存中汇集日志数据以减少日志量, 然后刷出到日志文件。
     * 
     * @param keys 汇总的目标, 通常与最后输出到日志的统计目标信息/fields 相同。
     * @param fields 日志记录字段, 代表最后输出到日志的内容和顺序。内容至少包含汇总目标/key.
     * @param values 输出的统计数据, 约定第一个值是数量, 后面的是统计值 (RT, 并发量, etc).
     */
    public abstract void write(Object[] keys, Object[] fields, // NL
                               long... values);

    /* 多个统计值的简便调用入口 */
    public final void log(Object key, long... values) {
        Object[] keys = new Object[] { key };
        write(keys, keys, values);
    }

    public final void log2(Object key1, Object key2, long... values) {
        Object[] keys = new Object[] { key1, key2 };
        write(keys, keys, values);
    }

    public final void log3(Object key1, Object key2, Object key3, long... values) {
        Object[] keys = new Object[] { key1, key2, key3 };
        write(keys, keys, values);
    }

    /* 次数/统计值的简便调用入口 */
    public final void stat(Object key, long value) {
        Object[] keys = new Object[] { key };
        write(keys, keys, new long[] { 1L, value });
    }

    public final void stat(Object key, long count, long value) {
        Object[] keys = new Object[] { key };
        write(keys, keys, new long[] { count, value });
    }

    public final void stat(Object key1, Object key2, long value) {
        Object[] keys = new Object[] { key1, key2 };
        write(keys, keys, new long[] { 1L, value });
    }

    public final void stat(Object key1, Object key2, long count, long value) {
        Object[] keys = new Object[] { key1, key2 };
        write(keys, keys, new long[] { count, value });
    }

    public final void stat(Object key1, Object key2, Object key3, long value) {
        Object[] keys = new Object[] { key1, key2, key3 };
        write(keys, keys, new long[] { 1L, value });
    }

    public final void stat(Object key1, Object key2, Object key3, // NL
                           long count, long value) {
        Object[] keys = new Object[] { key1, key2, key3 };
        write(keys, keys, new long[] { count, value });
    }
}
