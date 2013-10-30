package com.taobao.tddl.config;

import java.text.MessageFormat;

/**
 * 订阅持久化数据的辅助类
 * 
 * @author linxuan
 */
public final class ConfigServerHelper {

    public static final String DATA_ID_PREFIX             = "com.taobao.tddl.v1_";

    public static final String DATA_ID_TDDL_SHARD_RULE    = DATA_ID_PREFIX + "{0}_shardrule";

    public static final String DATA_ID_DB_GROUP_KEYS      = DATA_ID_PREFIX + "{0}_dbgroups";

    public static final String DATA_ID_TDDL_CLIENT_CONFIG = DATA_ID_PREFIX + "{0}_tddlconfig";

    public enum TddlLConfigKey {
        statKeyRecordType, statKeyLeftCutLen, statKeyRightCutLen, statKeyExcludes, StatRealDbInWrapperDs, //
        StatChannelMask, statDumpInterval/* 秒 */, statCacheSize, statAtomSql, statKeyIncludes, //
        SmoothValveProperties, CountPunisherProperties, //
        // add by junyu
        sqlExecTimeOutMilli/* sql超时时间 */, atomSqlSamplingRate/* atom层sql统计的采样率 */;
    }

    public static String getTddlConfigDataId(String appName) {
        return new MessageFormat(DATA_ID_TDDL_CLIENT_CONFIG).format(new Object[] { appName });
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
