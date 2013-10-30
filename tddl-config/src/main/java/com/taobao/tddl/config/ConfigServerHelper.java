package com.taobao.tddl.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Properties;

import com.taobao.tddl.config.impl.UnitConfigDataHandlerFactory;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 订阅持久化数据的辅助类
 * 
 * @author linxuan
 */
public final class ConfigServerHelper {

    private static final Logger logger                       = LoggerFactory.getLogger(ConfigServerHelper.class);

    public static final int     SUBSCRIBE_REGISTER_WAIT_TIME = 30000;

    public static final String  DATA_ID_PREFIX               = "com.taobao.tddl.v1_";

    public static final String  DATA_ID_TDDL_SHARD_RULE      = DATA_ID_PREFIX + "{0}_shardrule";

    public static final String  DATA_ID_DB_GROUP_KEYS        = DATA_ID_PREFIX + "{0}_dbgroups";

    public static final String  DATA_ID_TDDL_CLIENT_CONFIG   = DATA_ID_PREFIX + "{0}_tddlconfig";

    public static String getTddlConfigDataId(String appName) {
        return new MessageFormat(DATA_ID_TDDL_CLIENT_CONFIG).format(new Object[] { appName });
    }

    public enum TddlLConfigKey {
        statKeyRecordType, statKeyLeftCutLen, statKeyRightCutLen, statKeyExcludes, StatRealDbInWrapperDs, //
        StatChannelMask, statDumpInterval/* 秒 */, statCacheSize, statAtomSql, statKeyIncludes, //
        SmoothValveProperties, CountPunisherProperties, //
        // add by junyu
        sqlExecTimeOutMilli/* sql超时时间 */, atomSqlSamplingRate/* atom层sql统计的采样率 */;
    }

    /**
     * 订阅应用的分库分表配置
     */
    public static Object subscribeShardRuleConfig(String appName, DataListener listener) {
        if (appName == null || appName.length() == 0) {
            throw new IllegalStateException("没有指定应用名称appName");
        }
        String dataId = new MessageFormat(DATA_ID_TDDL_SHARD_RULE).format(new Object[] { appName });
        return ConfigServerHelper.subscribePersistentData(appName, getCallerClassName(), dataId, listener);
    }

    /**
     * 订阅其他TDDL客户端配置
     */
    public static Object subscribeTDDLConfig(String appName, DataListener listener) {
        if (appName == null || appName.length() == 0) {
            throw new IllegalStateException("没有指定应用名称appName");
        }
        String dataId = getTddlConfigDataId(appName);
        return ConfigServerHelper.subscribePersistentData(appName, getCallerClassName(), dataId, listener);
    }

    private static String getCallerClassName() {
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();
        return stes[stes.length - 1].getClassName();
    }

    /**
     * @return 第一次获取的data；返回时onDataReceiveAtRegister已经调用过一次
     */
    public static Object subscribePersistentData(String dataId, final DataListener listener) {
        return subscribePersistentData(getCallerClassName(), dataId, listener);
    }

    public static Object subscribeData(String dataId, final DataListener listener) {
        // 订阅非持久数据，和订阅持久数据的接口是完全相同的
        return subscribePersistentData(getCallerClassName(), dataId, listener);
    }

    private volatile static ConfigDataHandlerFactory cdhf;
    private static final long                        DIAMOND_FIRST_DATA_TIMEOUT = 15 * 1000;

    public static Object subscribePersistentData(String subscriberName, String dataId, final DataListener listener) {
        return subscribePersistentData(null, subscriberName, dataId, listener);
    }

    public static Object subscribePersistentData(String appName, String subscriberName, String dataId,
                                                 final DataListener listener) {
        cdhf = new UnitConfigDataHandlerFactory(UnitConfigDataHandlerFactory.DEFAULT_UNITNAME, appName);
        ConfigDataHandler matrixHandler = cdhf.getConfigDataHandler(dataId);
        String datas = matrixHandler.getNullableData(DIAMOND_FIRST_DATA_TIMEOUT,
            ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY); // 取配置信息的默认超时时间为30秒

        // 尝试去拿最新的本地数据
        logger.warn(dataId + "'s firstData=" + datas);
        if (datas != null) {
            try {
                listener.onDataReceiveAtRegister(datas);
            } catch (Throwable t) {
                // 保证即使首次处理dataId发生异常，listener也一样会被注册，业务仍然能收到后续推送
                logger.error("onDataReceiveAtRegister抛出异常，dataId:" + dataId, t);
            }
        }
        matrixHandler.addListener(new ConfigDataListener() {

            @Override
            public void onDataRecieved(String dataId, String data) {
                logger.info("recieve data,data id:" + dataId + " data:" + data);
                listener.onDataReceive(data);
            }
        }, null);

        return datas;
    }

    /**
     * 一个Util方法，不想再搞个Util类，故放到这里： 将Properties对象或Properties字符串解析为Properties对象
     */
    public static Properties parseProperties(Object data, String msg) {
        Properties p;
        if (data == null) {
            logger.warn(msg + "data == null");
            return null;
        } else if (data instanceof Properties) {
            p = (Properties) data;
        } else if (data instanceof String) {
            p = new Properties();
            try {
                p.load(new ByteArrayInputStream(((String) data).getBytes()));
            } catch (IOException e) {
                logger.error(msg + "无法解析推送的配置：" + data, e);
                return null;
            }
        } else {
            logger.warn(msg + "类型无法识别" + data);
            return null;
        }
        return p;
    }

    // 测试辅助方法，发布一个持久数据
    public static void publish(String dataId, Serializable data) {
        publish(dataId, data, null);
    }

    public static void publish(String dataId, Serializable data, String group) {
        return;
        // Diamond要用额外的sdk发布数据。如果测试还是用mock
        // TODO 改成sdk方式，支持测试
    }

    public static interface DataListener {

        /**
         * 注册之后，设置DataObserver之前，fetchConfig接受到推送时，调用该方法。一般用在业务初始化时
         */
        void onDataReceiveAtRegister(Object data);

        /**
         * 注册之后，第一次接收到推送调用onDataReceiveAtRegister处理完毕，
         * 设置DataObserver之后，再接到动态推送，调用该方法。一般用在业务运行时
         */
        void onDataReceive(Object data);
    }

    public static abstract class AbstractDataListener implements DataListener {

        public void onDataReceiveAtRegister(Object data) {
            this.onDataReceive(data);
        }
    }

    public static String getDBGroupsConfig(String appName) {
        if (appName == null || appName.length() == 0) {
            throw new IllegalStateException("没有指定应用名称appName");
        }
        String dataId = new MessageFormat(DATA_ID_DB_GROUP_KEYS).format(new Object[] { appName });
        return dataId;
    }

    public static String getShardRuleConfig(String appName) {
        if (appName == null || appName.length() == 0) {
            throw new IllegalStateException("没有指定应用名称appName");
        }
        String dataId = new MessageFormat(DATA_ID_TDDL_SHARD_RULE).format(new Object[] { appName });
        return dataId;
    }
}
