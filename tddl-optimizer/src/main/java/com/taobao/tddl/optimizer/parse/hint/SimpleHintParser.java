package com.taobao.tddl.optimizer.parse.hint;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeAND;
import com.taobao.tddl.rule.model.sqljep.ComparativeBaseList;
import com.taobao.tddl.rule.model.sqljep.ComparativeOR;
import com.taobao.tddl.rule.utils.ComparativeStringAnalyser;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 简单hint解析
 * 
 * <pre>
 * 完整的例子：
 * 
 *  \/*+TDDL({"type":"condition","vtab":"vtabxxx","params":[{"relation":"and","expr":["pk>4","pk:long<10"],"paramtype":"int"}],"extra":"{"ALLOW_TEMPORARY_TABLE"="TRUE"})*\/
 * 1. type取值是condition和direct
 * 2. vtab取值是rule规则中的逻辑表名
 * 3. params对应于rule规则中的分库字段的条件
 *    a. relation取值是and和or,可以不用，但是expr里面元素必须为一个。（即where pk>4）
 *    b. expr对应为分库条件
 *    c. paramtype对应分库子段类型
 * 4. extra取值是针对{@linkplain ExtraCmd}中定义的扩展参数，比如ALLOW_TEMPORARY_TABLE=true代表开启临时表
 * 
 * type为direct时
 * a. \/*+TDDL({"type":"direct","vtab":"real_tab","dbid":"xxx_group","realtabs":["real_tab_0","real_tab_1"]})*\/select * from real_tab;
 * 绕过解析器, 进行表名替换,然后在对应group ds上执行
 * b. \/*+TDDL({"type":"direct","dbid":"xxx_group"})*\/select * from real_table_0;
 * 直接将sql在对应group ds上执行
 * </pre>
 * 
 * @author jianghang 2014-1-13 下午6:16:31
 * @since 5.0.0
 */
public class SimpleHintParser {

    private static final Logger logger    = LoggerFactory.getLogger(SimpleHintParser.class);
    private static final String OR        = "or";
    private static final String AND       = "and";
    private static final String RELATION  = "relation";
    private static final String PARAMTYPE = "paramtype";
    private static final String EXPR      = "expr";
    private static final String PARAMS    = "params";
    private static final String VTAB      = "vtab";
    private static final String DBID      = "dbid";
    private static final String REALTABS  = "realtabs";
    private static final String EXTRACMD  = "extra";

    public static RouteCondition convertHint2RouteCondition(String sql, Map<Integer, ParameterContext> parameterSettings) {
        String tddlHint = extractHint(sql, parameterSettings);
        if (StringUtils.isNotEmpty(tddlHint)) {
            try {
                JSONObject jsonObject = JSON.parseObject(tddlHint);
                String type = jsonObject.getString("type");
                if ("direct".equalsIgnoreCase(type)) {
                    return decodeDirect(jsonObject);
                } else if ("condition".equalsIgnoreCase(type)) {
                    return decodeCondition(jsonObject);
                } else {
                    return decodeExtra(jsonObject);
                }
            } catch (JSONException e) {
                logger.error("convert tddl hint to RouteContion faild,check the hint string!", e);
                throw e;
            }

        }

        return null;
    }

    public static RouteCondition decodeDirect(JSONObject jsonObject) {
        DirectlyRouteCondition rc = new DirectlyRouteCondition();
        decodeExtra(rc, jsonObject);
        String tableString = containsKvNotBlank(jsonObject, REALTABS);
        if (tableString != null) {
            JSONArray jsonTables = JSON.parseArray(tableString);
            // 设置table的Set<String>
            if (jsonTables.size() > 0) {
                Set<String> tables = new HashSet<String>(jsonTables.size());
                for (int i = 0; i < jsonTables.size(); i++) {
                    tables.add(jsonTables.getString(i));
                }
                rc.setTables(tables);
                // direct只需要在实际表有的前提下解析即可。
                decodeVtab(rc, jsonObject);
            }
        }

        String dbId = containsKvNotBlank(jsonObject, DBID);
        if (dbId == null) {
            throw new RuntimeException("hint contains no property 'dbid'.");
        }

        rc.setDBId(dbId);
        return rc;
    }

    private static void decodeVtab(RouteCondition rc, JSONObject jsonObject) throws JSONException {
        String virtualTableName = containsKvNotBlank(jsonObject, VTAB);
        if (virtualTableName == null) {
            throw new TddlRuntimeException("hint contains no property 'vtab'.");
        }

        rc.setVirtualTableName(virtualTableName);
    }

    private static ExtraCmdRouteCondition decodeExtra(JSONObject jsonObject) throws JSONException {
        ExtraCmdRouteCondition rc = new ExtraCmdRouteCondition();
        String extraCmd = containsKvNotBlank(jsonObject, EXTRACMD);
        if (StringUtils.isNotEmpty(extraCmd)) {
            JSONObject extraCmds = JSON.parseObject(extraCmd);
            for (Map.Entry<String, Object> entry : extraCmds.entrySet()) {
                rc.getExtraCmds().put(StringUtils.upperCase(entry.getKey()), entry.getValue());
            }
        }
        return rc;
    }

    private static void decodeExtra(RouteCondition rc, JSONObject jsonObject) throws JSONException {
        String extraCmd = containsKvNotBlank(jsonObject, EXTRACMD);
        if (StringUtils.isNotEmpty(extraCmd)) {
            JSONObject extraCmds = JSON.parseObject(extraCmd);
            rc.getExtraCmds().putAll(extraCmds);
        }
    }

    public static RouteCondition decodeCondition(JSONObject jsonObject) {
        RuleRouteCondition sc = new RuleRouteCondition();
        decodeVtab(sc, jsonObject);
        decodeExtra(sc, jsonObject);
        String paramsStr = containsKvNotBlank(jsonObject, PARAMS);
        if (paramsStr != null) {
            JSONArray params = JSON.parseArray(paramsStr);
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    JSONObject o = params.getJSONObject(i);
                    JSONArray exprs = o.getJSONArray(EXPR);
                    String paramtype = o.getString(PARAMTYPE);
                    if (o.containsKey(RELATION)) {
                        String relation = o.getString(RELATION);
                        ComparativeBaseList comList = null;
                        if (relation != null && AND.equals(relation)) {
                            comList = new ComparativeAND();
                        } else if (relation != null && OR.equals(relation)) {
                            comList = new ComparativeOR();
                        } else {
                            throw new TddlRuntimeException("multi param but no relation,the hint is:" + sc.toString());
                        }

                        String key = null;
                        for (int j = 0; j < exprs.size(); j++) {
                            Comparative comparative = ComparativeStringAnalyser.decodeComparative(exprs.getString(j),
                                paramtype);
                            comList.addComparative(comparative);

                            String temp = ComparativeStringAnalyser.decodeComparativeKey(exprs.getString(j));
                            if (null == key) {
                                key = temp;
                            } else if (!temp.equals(key)) {
                                throw new TddlRuntimeException("decodeCondition not support one relation with multi key,the relation is:["
                                                               + relation + "],expr list is:[" + exprs.toString());
                            }
                        }
                        sc.put(key, comList);
                    } else {
                        if (exprs.size() == 1) {
                            String key = ComparativeStringAnalyser.decodeComparativeKey(exprs.getString(0));
                            Comparative comparative = ComparativeStringAnalyser.decodeComparative(exprs.getString(0),
                                paramtype);
                            sc.put(key, comparative);
                        } else {
                            throw new TddlRuntimeException("relation neither 'and' nor 'or',but expr size is not 1");
                        }
                    }
                }
            }
        }

        return sc;
    }

    /**
     * 从sql中解出hint,并且将hint里面的?替换为参数的String形式
     * 
     * @param sql
     * @param parameterSettings
     * @return
     */
    public static String extractHint(String sql, Map<Integer, ParameterContext> parameterSettings) {
        String tddlHint = TStringUtil.getBetween(sql, "/*+TDDL(", ")*/");
        if (null == tddlHint || "".endsWith(tddlHint)) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        int size = tddlHint.length();
        int parameters = 1;
        for (int i = 0; i < size; i++) {
            if (tddlHint.charAt(i) == '?') {
                // TDDLHINT只能设置简单值
                if (parameterSettings == null) {
                    throw new TddlRuntimeException("hint中使用了'?'占位符,却没有设置setParameter()");
                }

                ParameterContext param = parameterSettings.get(parameters);
                sb.append(param.getArgs()[1]);
                // if (param.getParameterMethod() == ParameterMethod.setString)
                // {
                // sb.append("'");
                // sb.append(parameterSettings.get(parameters).getArgs()[1]);
                // sb.append("'");
                // } else {
                // sb.append(parameterSettings.get(parameters).getArgs()[1]);
                // }
                parameters++;
            } else {
                sb.append(tddlHint.charAt(i));
            }
        }
        return sb.toString();
    }

    public static String extractTDDLGroupHintString(String sql) {
        return TStringUtil.getBetween(sql, "/*+TDDL_GROUP({", "})*/");
    }

    public static String removeHint(String originsql, Map<Integer, ParameterContext> parameterSettings) {
        String sql = originsql;
        String tddlHint = TStringUtil.getBetween(sql, "/*+TDDL(", ")*/");
        if (null == tddlHint || "".endsWith(tddlHint)) {
            return originsql;
        }
        int size = tddlHint.length();
        int parameters = 0;
        for (int i = 0; i < size; i++) {
            if (tddlHint.charAt(i) == '?') {
                parameters++;
            }
        }

        sql = TStringUtil.removeBetweenWithSplitor(sql, "/*+TDDL(", ")*/");
        // TDDL的hint必需写在SQL语句的最前面，如果和ORACLE hint一起用，
        // 也必需写在hint字符串的最前面，否则参数非常难以处理，也就会出错

        // 如果parameters为0，说明TDDLhint中没有参数，所以直接返回sql即可
        if (parameters == 0) {
            return sql;
        }

        for (int i = 1; i <= parameters; i++) {
            parameterSettings.remove(i);
        }

        Map<Integer, ParameterContext> newParameterSettings = new TreeMap<Integer, ParameterContext>();
        for (Map.Entry<Integer, ParameterContext> entry : parameterSettings.entrySet()) {
            // 重新计算一下parameters index
            newParameterSettings.put(entry.getKey() - parameters, entry.getValue());
            entry.getValue().getArgs()[0] = entry.getKey() - parameters;// args里的第一位也是下标
        }

        parameterSettings.clear();
        parameterSettings.putAll(newParameterSettings);
        return sql;
    }

    private static String containsKvNotBlank(JSONObject jsonObject, String key) throws JSONException {
        if (!jsonObject.containsKey(key)) {
            return null;
        }

        String value = jsonObject.getString(key);
        if (TStringUtil.isBlank(value)) {
            return null;
        }
        return value;
    }
}
