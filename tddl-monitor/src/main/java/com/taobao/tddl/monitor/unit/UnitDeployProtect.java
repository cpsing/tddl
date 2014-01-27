package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

/**
 * @author mengshi.sunmengshi 2013-11-13 下午4:41:53
 * @since 5.0.0
 */
public class UnitDeployProtect {

    static TddlUnitDeployProtect delegate = null;
    static {
        delegate = ExtensionLoader.load(TddlUnitDeployProtect.class);
    }

    public static void unitDeployProtect(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        delegate.unitDeployProtect(sql, params);
    }

    public static void unitDeployProtect(String sql) throws SQLException {
        UnitDeployProtect.unitDeployProtect(sql, null);
    }

    public static void unitDeployProtect() throws SQLException {
        UnitDeployProtect.unitDeployProtect(null);
    }

    protected static void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params)
                                                                                                       throws UnitDeployInvalidException,
                                                                                                       SQLException {

        delegate.unitDeployProtectWithCause(sql, params);
    }

    protected static void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException {

        delegate.eagleEyeRecord(result, c);
    }

    protected static Object getValidKeyFromThread() {

        return delegate.getValidKeyFromThread();

    }

    public static void clearUnitValidThreadLocal() {

        delegate.clearUnitValidThreadLocal();
    }

    public static Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.getValidFromHint(sql, params);
    }

    public static String tryRemoveUnitValidHintAndParameter(String sql) {
        return tryRemoveUnitValidHintAndParameter(sql, null);
    }

    protected static String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        return delegate.tryRemoveUnitValidHintAndParameter(sql, params);
    }

    protected static String replaceWithParams(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.replaceWithParams(sql, params);
    }
}
