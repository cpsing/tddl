package com.taobao.tddl.monitor.unit;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

public class DefaultTddlUnitDeployProtect implements TddlUnitDeployProtect {

    @Override
    public void unitDeployProtect(String sql, Map<Integer, ParameterContext> params) {
        // TODO Auto-generated method stub

    }

    @Override
    public void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params) {
        // TODO Auto-generated method stub

    }

    @Override
    public void eagleEyeRecord(boolean result, Object c) {
        // TODO Auto-generated method stub

    }

    @Override
    public Object getValidKeyFromThread() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearUnitValidThreadLocal() {
        // TODO Auto-generated method stub

    }

    @Override
    public Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        // TODO Auto-generated method stub
        return sql;
    }

    @Override
    public String replaceWithParams(String sql, Map<Integer, ParameterContext> params) {
        // TODO Auto-generated method stub
        return sql;
    }

}
