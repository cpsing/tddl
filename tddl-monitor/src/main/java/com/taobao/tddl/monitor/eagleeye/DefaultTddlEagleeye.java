package com.taobao.tddl.monitor.eagleeye;

import com.taobao.tddl.common.model.SqlMetaData;

public class DefaultTddlEagleeye implements TddlEagleeye {

    @Override
    public void startRpc(String ip, String port, String dbName, String sqlType) {
        // TODO Auto-generated method stub

    }

    @Override
    public void endSuccessRpc(String sql) {
        // TODO Auto-generated method stub

    }

    @Override
    public void endFailedRpc(String sql) {
        // TODO Auto-generated method stub

    }

    @Override
    public void endRpc(SqlMetaData sqlMetaData, Exception e) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getUserData(String key) {
        // TODO Auto-generated method stub
        return null;
    }

}
