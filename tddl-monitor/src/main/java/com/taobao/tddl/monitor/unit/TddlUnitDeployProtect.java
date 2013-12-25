package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

public interface TddlUnitDeployProtect {

    void unitDeployProtect(String sql, Map<Integer, ParameterContext> params) throws SQLException;

    void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params)
                                                                                      throws UnitDeployInvalidException,
                                                                                      SQLException;

    void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException;

    Object getValidKeyFromThread();

    void clearUnitValidThreadLocal();

    Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException;

    String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params);

    String replaceWithParams(String sql, Map<Integer, ParameterContext> params) throws SQLException;

}
