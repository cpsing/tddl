package com.taobao.tddl.monitor.unit;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

public interface TUnitDeployProtect {

    void unitDeployProtect(String sql, Map<Integer, ParameterContext> params);

    void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params);

    void eagleEyeRecord(boolean result, Object c);

    Object getValidKeyFromThread();

    void clearUnitValidThreadLocal();

    Object getValidFromHint(String sql, Map<Integer, ParameterContext> params);

    String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params);

    String replaceWithParams(String sql, Map<Integer, ParameterContext> params);

}
