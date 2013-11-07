package com.taobao.tddl.monitor.eagleeye;

import com.taobao.tddl.common.model.SqlMetaData;

public interface TddlEagleeye {

    /**
     * execute之前写日志
     * 
     * @param datasourceWrapper
     * @param sqlType
     * @throws Exception
     */
    public void startRpc(String ip, String port, String dbName, String sqlType);

    /**
     * execute成功之后写日志
     */
    public void endSuccessRpc(String sql);

    /**
     * execute失败之后写日志
     */
    public void endFailedRpc(String sql);

    /**
     * @param sqlMetaData
     * @param e
     */
    public void endRpc(SqlMetaData sqlMetaData, Exception e);

    /**
     * @param key
     * @return
     */
    public String getUserData(String key);

}
