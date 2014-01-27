package com.taobao.tddl.monitor;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.ConfigServerHelper;
import com.taobao.tddl.config.ConfigServerHelper.TddlLConfigKey;
import com.taobao.tddl.config.impl.UnitConfigDataHandlerFactory;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.stat.BufferedLogWriter;
import com.taobao.tddl.monitor.stat.LoggerLogWriter;
import com.taobao.tddl.monitor.stat.MinMaxAvgLogWriter;
import com.taobao.tddl.monitor.stat.SoftRefLogWriter;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 维护Monitor需要的参数
 * 
 * @author jianghang 2013-10-30 下午5:49:16
 * @since 5.0.0
 */
public class MonitorConfig {

    static final Logger                   logger                      = LoggerFactory.getLogger(Monitor.class);                      // 使用monitor.class，兼容
    public static volatile String         APPNAME                     = "TDDL";
    public static volatile Boolean        isStatRealDbInWrapperDs     = null;
    public static volatile RECORD_TYPE    recordType                  = RECORD_TYPE.RECORD_SQL;
    public static volatile int            left                        = 0;                                                           // 从左起保留多少个字符
    public static volatile int            right                       = 0;                                                           // 从右起保留多少个字符
    public static volatile String[]       excludsKeys                 = null;
    public static volatile String[]       includeKeys                 = null;                                                        // 白名单
    // modify by junyu,2012-3-28
    public static volatile boolean        isStatAtomSql               = true;                                                        // 默认不打印sql日志
    public static volatile int            sqlTimeout                  = 500;                                                         // 默认超时500毫秒
    public static volatile int            atomSamplingRate            = 100;                                                         // 值只能为0-100,日志的采样频率
    public static volatile int            statChannelMask             = 7;                                                           // 按位：哈勃|BufferedStatLogWriter|StatMonitor
    public static volatile int            dumpInterval                = -1;
    public static volatile int            cacheSize                   = -1;

    public static final StatMonitor       statMonitor                 = StatMonitor.getInstance();

    /** changyuan.lh: TDDL 统计日志 */
    /* 记录行复制日志与 SQL 解析日志, Key 的量与 SQL 数量相同 */
    public static final BufferedLogWriter bufferedStatLogWriter       = new BufferedLogWriter(1024,
                                                                          4096,
                                                                          new LoggerLogWriter(LoggerInit.TDDL_Statistic_LOG));
    /* 记录单库的 SQL 执行记录, Key 的量是 SQL x 单库物理表 x 物理库数量 */
    public static final BufferedLogWriter atomBufferedStatLogWriter   = new BufferedLogWriter(2048,
                                                                          131072,
                                                                          new LoggerLogWriter(LoggerInit.TDDL_Atom_Statistic_LOG));
    /* 记录逻辑表以及物理库/物理表 的 SQL 执行记录, Key 量最大是 SQL x 单库物理表 x 物理库数量 */
    public static final BufferedLogWriter matrixBufferedStatLogWriter = new BufferedLogWriter(2048,
                                                                          131072,
                                                                          new LoggerLogWriter(LoggerInit.TDDL_Matrix_Statistic_LOG));
    /* 记录 Atom 连接池以及业务分桶的连接申请记录, Key 量最大是物理库 x 业务分桶数量 */
    public static final SoftRefLogWriter  connRefStatLogWriter        = new SoftRefLogWriter(false,
                                                                          new MinMaxAvgLogWriter(", ",
                                                                              LoggerInit.TDDL_Conn_Statistic_LOG));

    public enum RECORD_TYPE {
        RECORD_SQL, MD5, NONE
    }

    static void initConfig() {
        if ("TDDL".equals(APPNAME)) {
            logger.warn("不指定TDDL以外的appName则不订阅");
            return;
        }

        String dataId = ConfigServerHelper.getTddlConfigDataId(APPNAME);
        ConfigDataHandlerFactory cdhf = new UnitConfigDataHandlerFactory(UnitConfigDataHandlerFactory.DEFAULT_UNITNAME,
            APPNAME);

        ConfigDataListener tddlConfigListener = new ConfigDataListener() {

            public void onDataRecieved(String dataId, String data) {
                if (StringUtils.isEmpty(data)) {
                    return;
                }

                Properties p = new Properties();
                try {
                    p.load(new ByteArrayInputStream(data.getBytes()));
                } catch (Exception e) {
                    logger.error("[tddlConfigListener] 无法解析推送的配置,dataid:" + dataId + ", data:" + data, e);
                    return;
                }

                try {
                    for (Map.Entry<Object, Object> entry : p.entrySet()) {
                        String key = ((String) entry.getKey()).trim();
                        String value = ((String) entry.getValue()).trim();
                        switch (TddlLConfigKey.valueOf(key)) {
                            case statKeyRecordType: {
                                RECORD_TYPE old = recordType;
                                recordType = RECORD_TYPE.valueOf(value);
                                logger.warn("statKeyRecordType switch from [" + old + "] to [" + recordType + "]");
                                break;
                            }
                            case statKeyLeftCutLen: {
                                int old = left;
                                left = Integer.valueOf(value);
                                logger.warn("statKeyLeftCutLen switch from [" + old + "] to [" + left + "]");
                                break;
                            }
                            case statKeyRightCutLen: {
                                int old = right;
                                right = Integer.valueOf(value);
                                logger.warn("statKeyRightCutLen switch from [" + old + "] to [" + right + "]");
                                break;
                            }
                            case statKeyExcludes: {
                                String[] old = excludsKeys;
                                excludsKeys = value.split(",");
                                logger.warn("statKeyExcludes switch from " + Arrays.toString(old) + " to [" + value
                                            + "]");
                                break;
                            }
                            case statKeyIncludes: {
                                String[] old = includeKeys;
                                includeKeys = value.split(",");
                                logger.warn("statKeyIncludes switch from " + Arrays.toString(old) + " to [" + value
                                            + "]");
                                break;
                            }
                            case StatRealDbInWrapperDs: {
                                boolean old = isStatRealDbInWrapperDs;
                                isStatRealDbInWrapperDs = Boolean.valueOf(value);
                                logger.warn("StatRealDbInWrapperDs switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case StatChannelMask: {
                                int old = statChannelMask;
                                statChannelMask = Integer.valueOf(value);
                                logger.warn("statChannelMask switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case statDumpInterval: {
                                int old = dumpInterval;
                                dumpInterval = Integer.valueOf(value);
                                statMonitor.setFlushInterval(dumpInterval);
                                bufferedStatLogWriter.setFlushInterval(dumpInterval);
                                logger.warn("statDumpInterval switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case statCacheSize: {
                                int old = cacheSize;
                                cacheSize = Integer.valueOf(value);
                                statMonitor.setMaxKeySize(cacheSize);
                                bufferedStatLogWriter.setMaxKeySize(cacheSize);
                                logger.warn("statCacheSize switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case statAtomSql: {
                                boolean old = isStatAtomSql;
                                isStatAtomSql = Boolean.parseBoolean(value);
                                logger.warn("isStatAtomSql switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case sqlExecTimeOutMilli: {
                                int old = sqlTimeout;
                                sqlTimeout = Integer.valueOf(value);
                                logger.warn("sqlTimeout switch from [" + old + "] to [" + value + "]");
                                break;
                            }
                            case atomSqlSamplingRate: {
                                int old = atomSamplingRate;
                                if (old > 0) {
                                    int rate = 0;
                                    if (Integer.valueOf(value) % 100 == 0) {
                                        rate = 100;
                                    } else {
                                        rate = Integer.valueOf(value) % 100;// 如果超过100,取余量
                                    }
                                    atomSamplingRate = rate;
                                    logger.warn("atomSqlSamplingRate switch from [" + old + "] to [" + atomSamplingRate
                                                + "]");
                                } else {
                                    logger.warn("atomSqlSamplingRate will not change,because the value got is nagetive!old value is:"
                                                + old);
                                }
                            }
                            default:
                                logger.warn("Not cared TDDLConfigKey:" + key);
                        }
                    }
                } catch (Exception e) {
                    logger.error("[tddlConfigListener.onDataReceive]", e);
                }

            }

        };
        ConfigDataHandler dataHandler = cdhf.getConfigDataHandler(dataId, tddlConfigListener);
        String data = dataHandler.getNullableData(15 * 1000, ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
        tddlConfigListener.onDataRecieved(dataId, data);
    }

    public static void setAppName(String appname) {
        if (appname != null) {
            APPNAME = appname;
            initConfig();
        }
    }
}
