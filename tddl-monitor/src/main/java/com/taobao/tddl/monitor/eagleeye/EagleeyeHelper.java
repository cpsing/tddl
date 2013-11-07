package com.taobao.tddl.monitor.eagleeye;

import com.taobao.tddl.common.model.SqlMetaData;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.monitor.eagleeye.TddlEagleeye;

/**
 * @author mengshi.sunmengshi
 */
public class EagleeyeHelper {

    static TddlEagleeye eagleeye = null;
    static {
        eagleeye = ExtensionLoader.load(TddlEagleeye.class);
    }

    /**
     * execute之前写日志
     * 
     * @param datasourceWrapper
     * @param sqlType
     * @throws Exception
     */
    public static void startRpc(String ip, String port, String dbName, String sqlType) {
        eagleeye.startRpc(ip, port, dbName, sqlType);
    }

    /**
     * execute成功之后写日志
     */
    public static void endSuccessRpc(String sql) {
        eagleeye.endSuccessRpc(sql);
    }

    /**
     * execute失败之后写日志
     */
    public static void endFailedRpc(String sql) {
        eagleeye.endFailedRpc(sql);
    }

    /**
     * @param sqlMetaData
     * @param e
     */
    public static void endRpc(SqlMetaData sqlMetaData, Exception e) {
        eagleeye.endRpc(sqlMetaData, e);
    }

    public static String getUserData(String key) {
        return eagleeye.getUserData(key);
    }

}
