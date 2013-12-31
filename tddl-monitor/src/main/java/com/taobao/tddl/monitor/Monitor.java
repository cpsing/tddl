package com.taobao.tddl.monitor;

import com.taobao.tddl.common.utils.GoogleConcurrentLruCache;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.stat.AbstractStatLogWriter.LogCounter;
import com.taobao.tddl.monitor.utils.MD5Maker;
import com.taobao.tddl.monitor.utils.PositiveAtomicCounter;

public class Monitor extends MonitorConfig {

    public static final String                                    KEY1                                 = "TDDL";
    public static final String                                    KEY3_PARSE_SQL                       = "PARSE_SQL_SUCCESS";

    // 会记录走则引擎用的时间和总耗时
    public static final String                                    KEY3_GET_DB_AND_TABLES               = "GET_DB_ANDTABLES_SUCCESS";
    /**
     * 执行sql的总时间，包含真正数据库的执行时间和总时间
     */
    public static final String                                    KEY3_EXECUTE_A_SQL_SUCCESS           = "EXECUTE_A_SQL_SUCCESS";
    /**
     * 总共执行了几个库，几个表
     */
    public static final String                                    KEY3_EXECUTE_A_SQL_SUCCESS_DBTAB     = "EXECUTE_A_SQL_SUCCESS_DBTAB";
    /**
     * 执行sql的总时间，包含真正数据库的执行时间和总时间
     */
    public static final String                                    KEY3_EXECUTE_A_SQL_TIMEOUT           = "EXECUTE_A_SQL_TIMEOUT";

    public static final String                                    KEY3_EXECUTE_A_SQL_TIMEOUT_DBTAB     = "EXECUTE_A_SQL_TIMEOUT_DBTAB";

    public static final String                                    KEY3_EXECUTE_A_SQL_EXCEPTION         = "EXECUTE_A_SQL_WITH_EXCEPTION";

    public static final String                                    KEY3_EXECUTE_A_SQL_EXCEPTION_DBTAB   = "EXECUTE_A_SQL_WITH_EXCEPTION_DBTAB";

    /**
     * TDDL 数据库（分桶）连接数
     */
    public static final String                                    KEY3_CONN_NUMBER                     = "CONN_NUM";
    /**
     * TDDL 数据库（分桶）连接阻塞时间
     */
    public static final String                                    KEY3_CONN_BLOCKING                   = "CONN_BLOCKING";

    public static String                                          blockingExecution                    = "blockingExecution";

    public static String                                          RPCClientcloseResultSetHandler       = "RPCClient_closeResultSetHandler";
    public static String                                          RPCClientcommit                      = "RPCClient_commit";
    public static String                                          RPCClientrollback                    = "RPCClient_rollback";
    public static String                                          RPCClientfetchNext                   = "RPCClient_fetchNext";
    public static String                                          RPCClientfirst                       = "RPCClient_first";

    public static String                                          RPCClientpingFuture                  = "RPCClient_pingFuture";
    public static String                                          RPCClientexecuteFuture               = "RPCClient_executeFuture";
    public static String                                          RPCClientfetchNextFuture             = "RPCClient_fetchNextFuture";
    public static String                                          RPCClientfirstFuture                 = "RPCClient_firstFuture";

    public static String                                          RPCClientcloseResultSetHandlerFuture = "RPCClient_closeResultSetHandlerFuture";
    public static String                                          RPCClientcommitFuture                = "RPCClient_commitFuture";
    public static String                                          RPCClientrollbackFuture              = "RPCClient_rollbackFuture";

    public static String                                          AndOrExecutorParse                   = "AndOrExecutor_Parse";
    public static String                                          AndOrExecutorOptimize                = "AndOrExecutorOptimize";
    public static String                                          TDDL_EXECUTE                         = "AndOrExecutorExecute";
    public static String                                          AndOrExecutorExecuteFuture           = "AndOrExecutorExecuteFuture";
    public static String                                          QueryTdhsHandlerUseTdhsApi           = "QueryTdhsHandlerUseTdhsApi";

    /**
     * 异步发送请求后，到拿结果集的时间
     */
    public static String                                          AndOrExecutorExecuteFuture_getResult = "AndOrExecutorExecuteFuture";

    public static String                                          ServerExecuteOnOthers                = "Server_ExecuteOnOthers";
    public static String                                          ServerPut                            = "Server_Put";
    public static String                                          ServerQuery                          = "Server_Query";

    public static String                                          Key3Success                          = "success";
    public static String                                          Key3Fail                             = "fail";
    public static String                                          Key3FutureDone                       = "futureDone";
    public static String                                          Key3FutureGet                        = "futureGet";

    public static String                                          Key3True                             = "Key3True";
    public static String                                          Key3False                            = "Key3False";

    private static final GoogleConcurrentLruCache<String, String> sqlToMD5Map                          = new GoogleConcurrentLruCache<String, String>();
    private static MD5Maker                                       md5Maker                             = MD5Maker.getInstance();
    private final static PositiveAtomicCounter                    pc                                   = new PositiveAtomicCounter();

    static {
        LoggerInit.initTddlLog();
        MonitorConfig.initConfig();
    }

    /**
     * 返回构建的表名
     * 
     * @param virtualTableName
     * @return
     */
    public static String buildTableKey1(String virtualTableName) {
        return "" + virtualTableName; // 保证不返回null
    }

    /**
     * <pre>
     * 记录sql
     * 不记录sql
     * 记录前截取sql
     * 记录后截取sql
     * 记录md5
     * 
     * 先左后右
     * </pre>
     * 
     * @param sql
     * @return
     */
    public static String buildExecuteSqlKey2(String sql) {
        if (sql == null) {
            return "null";
        }
        switch (recordType) {
            case RECORD_SQL:
                String s = TStringUtil.fillTabWithSpace(sql);
                if (left > 0) {
                    s = TStringUtil.left(s, left);
                }
                if (right > 0) {
                    s = TStringUtil.right(s, right);
                }
                return s;
            case MD5:
                String s1 = TStringUtil.fillTabWithSpace(sql);
                if (left > 0) {
                    s1 = TStringUtil.left(s1, left);
                }
                if (right > 0) {
                    s1 = TStringUtil.right(s1, right);
                }
                String md5 = sqlToMD5Map.get(s1);
                if (md5 != null) {
                    return md5;
                } else {
                    String sqlmd5 = md5Maker.getMD5(s1);
                    StringBuilder sb = new StringBuilder();
                    sb.append("[md5]").append(sqlmd5).append(" [sql]").append(s1);
                    LoggerInit.TDDL_MD5_TO_SQL_MAPPING.warn(sb.toString());
                    sqlToMD5Map.put(s1, sqlmd5);
                    return sqlmd5;
                }
            case NONE:
                return "";
            default:
                throw new IllegalArgumentException("不符合要求的记录log类型! " + recordType);
        }

    }

    /**
     * 构建KEY1的完整表名
     * 
     * @param realDSKey
     * @param realTable
     * @return
     */
    public static String buildExecuteDBAndTableKey1(String realDSKey, String realTable) {
        StringBuilder sb = new StringBuilder();
        sb.append(KEY1).append("|").append(realDSKey).append("|").append(realTable);
        return sb.toString();
    }

    /**
     * @param key1 一般是逻辑表名，appname等
     * @param key2 一般是SQL
     * @param key3 一些成功、失败、超时、命中率等标志
     * @param value1 执行时间
     * @param value2 次数
     */
    public static void add(String key1, String key2, String key3, long value1, long value2) {
        if (isExclude(key1, key2, key3)) {
            return;
        }
        if ((statChannelMask & 4) == 4) { // 100
            // MonitorLog.addStat(key1, "", key3, value1, value2); // 哈勃日志暂时保留
        }
        if ((statChannelMask & 2) == 2) { // 010
            bufferedStatLogWriter.stat(key2, key1, key3, value1, value2); //
        }
        if ((statChannelMask & 1) == 1) { // 001
            addMonitor(key1, key2, key3, value1, value2); // 平均响应时间等动态监控Nagois
        }
    }

    /**
     * @param key1 数据库名字
     * @param key2 一般是SQL
     * @param key3 一些成功、失败、超时、命中率等标志，比如SQL_EXCEPTION,SQL_TIMEOUT,SQL_SUCCESS
     * @param key4 数据库ip
     * @param key5 数据库ort
     * @param key6 真实库名
     * @param value1 执行时间
     * @param value2 次数
     */
    public static void atomSqlAdd(String key1, String key2, String key3, String key4, String key5, String key6,
                                  long value1, long value2) {
        // changyuan.lh : 输出顺序是 key1(sql), key2(group), attach1, attach2,
        // attach3, key3(flag)
        atomBufferedStatLogWriter.write(new Object[] { key2, key1, key3 }, new Object[] { key2, key1, key4, key5, key6,
                key3 }, new long[] { value2, value1 });
    }

    /**
     * @param key1 一般是逻辑表名，appname等
     * @param key2 一般是SQL
     * @param key3 一些成功、失败、超时、命中率等标志
     * @param value1 执行时间
     * @param value2 次数
     */
    public static void matrixSqlAdd(String key1, String key2, String key3, long value1, long value2) {
        matrixBufferedStatLogWriter.stat(key2, key1, key3, value2, value1);
    }

    /**
     * 获得一个统计对象, 不用可以直接抛弃 <br/>
     * 举个例子：datasourceKey, "-", Monitor.KEY3_CONN_NUMBER
     * 
     * @param obj1
     * @param obj2
     * @param obj3
     * @return
     */
    public static LogCounter connStat(String obj1, String obj2, String obj3) {
        Object[] objs = new Object[] { obj1, obj2, obj3 };
        return connRefStatLogWriter.getCounter(objs, objs);
    }

    /**
     * 根据rate率判断是否针对atom进行采样
     * 
     * @return
     */
    public static boolean isSamplingRecord() {
        int ra = pc.incrementAndGet() % 100;
        if (ra < Monitor.atomSamplingRate) {
            return true;
        } else {
            return false;
        }
    }

    // ====================== helper method ==============================

    private static void addMonitor(String key1, String key2, String key3, long value1, long value2) {
        // 一段时间内插日志库的失败率和平均响应时间
    }

    private static boolean isExclude(String key1, String key2, String key3) {
        if (excludsKeys == null || excludsKeys.length == 0) {
            return false;
        }

        for (String exclude : excludsKeys) {
            if (key1.indexOf(exclude) != -1 || key2.indexOf(exclude) != -1 || key3.indexOf(exclude) != -1) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInclude(String sql) {
        if (includeKeys != null && includeKeys.length != 0) { // 存在白名单
            boolean discard = true;
            for (String whiteItem : includeKeys) {
                if (sql.indexOf(whiteItem) != -1) {
                    discard = false;
                    break;
                }
            }
            if (discard) {
                return false; // 不在白名单中，不输出日志，以减少日志量
            }
        }
        return true;
    }

    public static synchronized void addSnapshotValuesCallbask(SnapshotValuesOutputCallBack callbackList) {
        statMonitor.addSnapshotValuesCallbask(callbackList);
    }

    public static synchronized void removeSnapshotValuesCallback(SnapshotValuesOutputCallBack callbackList) {
        statMonitor.removeSnapshotValuesCallback(callbackList);
    }

    public static long monitorAndRenewTime(String key1, String key2, String key3, long count, long time) {
        bufferedStatLogWriter.stat(key1, key2, key3, count, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        return time;
    }

    public static long monitorAndRenewTime(String key1, String key2, String key3, long time) {
        bufferedStatLogWriter.stat(key1, key2, key3, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        return time;
    }

}
