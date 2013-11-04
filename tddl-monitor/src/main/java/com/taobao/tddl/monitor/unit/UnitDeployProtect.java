package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.alibaba.fastjson.JSONObject;
import com.taobao.tddl.common.model.ThreadLocalString;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.thread.ThreadLocalMap;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-5-8下午04:04:56
 */
public class UnitDeployProtect {

    public static void unitDeployProtect(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        try {
            UnitDeployProtect.unitDeployProtectWithCause(sql, params);
        } catch (UnitDeployInvalidException e) {
            throw new SQLException(e);
        }
    }

    public static void unitDeployProtect(String sql) throws SQLException {
        UnitDeployProtect.unitDeployProtect(sql, null);
    }

    public static void unitDeployProtect() throws SQLException {
        UnitDeployProtect.unitDeployProtect(null);
    }

    private static void unitDeployProtectWithCause(String sql, Map<Integer, ParameterContext> params)
                                                                                                     throws UnitDeployInvalidException,
                                                                                                     SQLException {
        // 提取threadlocal或者hint
        Object c = getValidKeyFromThread();
        boolean result = true;
        // 获取user_id,调用api接口判定是否抛异常
        if (c != null) {
            result = Router.isInCurrentUnit(Long.valueOf(String.valueOf(c)));
            eagleEyeRecord(result, c);
        } else if (sql != null) {
            c = getValidFromHint(sql, params);
            if (c != null) {
                result = Router.isInCurrentUnit(Long.valueOf(String.valueOf(c)));
                eagleEyeRecord(result, c);
            }
        } else {
            // no unit protected
        }
    }

    private static void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException {
        if (!result) {
            StringBuilder sb = new StringBuilder(String.valueOf(c));
            sb.append(Router.CROSS_UNIT_FLAG);
            EagleEye.attribute(Router.ROUTER_ID, sb.toString());
            throw new UnitDeployInvalidException("cross unit operate,value is " + String.valueOf(c));
        } else {
            EagleEye.attribute(Router.ROUTER_ID, String.valueOf(c));
        }
    }

    private static Object getValidKeyFromThread() {
        Object o = ThreadLocalMap.get(ThreadLocalString.UNIT_VALID);
        // thread local just get and remove
        // clearUnitValidThreadLocal();
        if (o == null) {
            return null;
        } else {
            return o;
        }
    }

    public static void clearUnitValidThreadLocal() {
        ThreadLocalMap.remove(ThreadLocalString.UNIT_VALID);
    }

    public static final String VALID_COL   = "column";
    public static final String VALID_VALUE = "value";

    public static Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        sql = replaceWithParams(sql, params);
        String hint = TStringUtil.getBetween(sql, "/*+UNIT_VALID(", ")*/");
        if (hint != null && !"".equals(hint.trim())) {
            JSONObject o = JSONObject.fromObject(hint);
            Object c = o.get(VALID_VALUE);
            return c;
        } else {
            return null;
        }
    }

    public static String tryRemoveUnitValidHintAndParameter(String sql) {
        return tryRemoveUnitValidHintAndParameter(sql, null);
    }

    private static String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        String s = TStringUtil.removeBetweenWithSplitorNotExistNull(sql, "/*+UNIT_VALID(", ")*/");
        if (s == null) {
            return sql;
        } else {
            if (params != null) {
                int startS = sql.indexOf("/*+UNIT_VALID(");
                int startE = startS + "/*+UNIT_VALID(".length();
                int end = sql.indexOf(")*/") - 1;
                int size = sql.length();
                List<Integer> r = new ArrayList<Integer>();
                int parameters = 1;
                for (int i = 0; i < size; i++) {
                    if (sql.charAt(i) == '?') {
                        if (i < end && i > startE) {
                            r.add(parameters);
                        }
                        parameters++;
                    }
                }

                SortedMap<Integer, ParameterContext> tempMap = new TreeMap<Integer, ParameterContext>();
                for (Integer i : r) {
                    params.remove(i);
                }

                tempMap.putAll(params);
                params.clear();

                // 这段需要性能优化
                int tempMapSize = tempMap.size();
                for (int i = 1; i <= tempMapSize; i++) {
                    Integer ind = tempMap.firstKey();
                    ParameterContext pc = tempMap.get(ind);
                    pc.getArgs()[0] = i;
                    params.put(i, pc);
                    tempMap.remove(ind);
                }
            }

            return s;
        }
    }

    private static String replaceWithParams(String sql, Map<Integer, ParameterContext> parameterSettings)
                                                                                                         throws SQLException {
        if (parameterSettings == null) {
            return sql;
        } else {
            StringBuffer sb = new StringBuffer();
            int size = sql.length();
            int parameters = 1;
            for (int i = 0; i < size; i++) {
                if (sql.charAt(i) == '?') {
                    if (parameterSettings.get(parameters) == null) {
                        throw new SQLException("parameter size not match the place holder in sql,the parameter size is "
                                               + parameterSettings.size() + ",but the sql is " + sql);
                    }
                    // TDDLHINT只能设置简单值
                    sb.append(parameterSettings.get(parameters).getArgs()[1]);
                    parameters++;
                } else {
                    sb.append(sql.charAt(i));
                }
            }
            return sb.toString();
        }
    }
}
