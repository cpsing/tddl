package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

/**
 * @author mengshi.sunmengshi 2013-11-13 下午4:41:53
 * @since 5.1.0
 */
public class UnitDeployProtect {

    static TUnitDeployProtect delegate = null;
    static {
        delegate = ExtensionLoader.load(TUnitDeployProtect.class);
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

    private static void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params) {

        delegate.unitDeployProtectWithCause(sql, params);
    }

    private static void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException {

        delegate.eagleEyeRecord(result, c);
    }

    private static Object getValidKeyFromThread() {

        return delegate.getValidKeyFromThread();

    }

    public static void clearUnitValidThreadLocal() {

        delegate.clearUnitValidThreadLocal();
    }

    public static final String VALID_COL   = "column";
    public static final String VALID_VALUE = "value";

    public static Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.getValidFromHint(sql, params);
    }

    public static String tryRemoveUnitValidHintAndParameter(String sql) {
        return tryRemoveUnitValidHintAndParameter(sql, null);
    }

    private static String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        return delegate.tryRemoveUnitValidHintAndParameter(sql, params);
    }

    private static String replaceWithParams(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.replaceWithParams(sql, params);
    }
}
