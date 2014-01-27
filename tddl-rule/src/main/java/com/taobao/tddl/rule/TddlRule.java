package com.taobao.tddl.rule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.taobao.tddl.rule.exceptions.RouteCompareDiffException;
import com.taobao.tddl.rule.model.Field;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeMapChoicer;
import com.taobao.tddl.rule.utils.ComparativeStringAnalyser;
import com.taobao.tddl.rule.utils.MatchResultCompare;

/**
 * 类名取名兼容老的rule代码<br/>
 * 结合tddl的动态规则管理体系，获取对应{@linkplain VirtualTableRule}
 * 规则定义，再根据sql中condition或者是setParam()提交的参数计算出路由规则 {@linkplain MatcherResult}
 * 
 * <pre>
 * condition简单语法： KEY CMP VALUE [:TYPE]
 * 1. KEY： 类似字段名字，用户随意定义
 * 2. CMP： 链接符，比如< = > 等，具体可查看{@linkplain Comparative}
 * 3. VALUE: 对应的值，比如1
 * 4. TYPE: 描述VALUE的类型，可选型，如果不填默认为Long类型。支持: int/long/string/date，可以使用首字母做为缩写，比如i/l/s/d。
 * 
 * 几个例子：
 * 1. id = 1
 * 2. id = 1 : long
 * 3. id > 1 and id < 1 : long
 * 4. gmt_create = 2011-11-11 : date
 * 5. id in (1,2,3,4) : long
 * </pre>
 * 
 * @author jianghang 2013-11-5 下午8:11:43
 * @since 5.0.0
 */
public class TddlRule extends TddlRuleConfig implements TddlTableRule {

    private VirtualTableRuleMatcher matcher = new VirtualTableRuleMatcher();

    public MatcherResult route(String vtab, String condition) {
        return route(vtab, condition, super.getCurrentRule());
    }

    public MatcherResult route(String vtab, String condition, String version) {
        return route(vtab, condition, super.getVersionRule(version));
    }

    public MatcherResult route(String vtab, String condition, VirtualTableRoot specifyVtr) {
        return route(vtab, generateComparativeMapChoicer(condition), Lists.newArrayList(), specifyVtr);
    }

    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args) {
        return route(vtab, choicer, args, super.getCurrentRule());
    }

    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args, String version) {
        return route(vtab, choicer, args, super.getVersionRule(version));
    }

    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRoot specifyVtr) {
        TableRule rule = specifyVtr.getVirtualTable(vtab);
        if (rule != null) {
            return matcher.match(choicer, args, rule, true);
        } else {
            // 不存在规则，返回默认的
            return defaultRoute(vtab, specifyVtr);
        }
    }

    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args) throws RouteCompareDiffException {
        if (super.getAllVersions().size() == 0) {
            throw new RuntimeException("routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
        }

        // 如果只有单套规则,直接返回这套规则的路由结果
        if (super.getAllVersions().size() == 1) {
            return route(vtab, choicer, args, super.getCurrentRule());
        }

        // 如果不止一套规则,那么计算两套规则,默认都返回新规则
        if (super.getAllVersions().size() != 2) {
            throw new RuntimeException("not support more than 2 copy rule compare");
        }

        // 第一个排位的为旧规则
        MatcherResult oldResult = route(vtab, choicer, args, super.getCurrentRule());
        if (isSelect) {
            return oldResult;
        } else {
            // 第二个排位的为新规则
            MatcherResult newResult = route(vtab, choicer, args, super.getVersionRule(super.getAllVersions().get(1)));
            boolean compareResult = MatchResultCompare.matchResultCompare(newResult, oldResult);
            if (compareResult) {
                return oldResult;
            } else {
                throw new RouteCompareDiffException("sql type is not-select,rule calculate result diff");
            }
        }
    }

    // ================ helper method ================

    /**
     * 没有分库分表的逻辑表，返回指定库表
     * 
     * @param vtab
     * @param vtrCurrent
     * @return
     */
    private MatcherResult defaultRoute(String vtab, VirtualTableRoot vtrCurrent) {
        TargetDB targetDb = new TargetDB();
        // 设置默认的链接库，比如就是groupKey
        targetDb.setDbIndex(this.getDefaultDbIndex(vtab, vtrCurrent));
        // 设置表名，同名不做转化
        Map<String, Field> tableNames = new HashMap<String, Field>(1);
        tableNames.put(vtab, null);
        targetDb.setTableNames(tableNames);

        return new MatcherResult(Arrays.asList(targetDb),
            new HashMap<String, Comparative>(),
            new HashMap<String, Comparative>());
    }

    /**
     * 没有分库分表的逻辑表，先从dbIndex中获取映射的库，没有则返回默认的库
     * 
     * @param vtab
     * @param vtrCurrent
     * @return
     */
    private String getDefaultDbIndex(String vtab, VirtualTableRoot vtrCurrent) {
        Map<String, String> dbIndexMap = vtrCurrent.getDbIndexMap();
        if (dbIndexMap != null && dbIndexMap.get(vtab) != null) {
            return dbIndexMap.get(vtab);
        }
        return vtrCurrent.getDefaultDbIndex();
    }

    protected ComparativeMapChoicer generateComparativeMapChoicer(String condition) {
        Map<String, Comparative> comparativeMap = ComparativeStringAnalyser.decodeComparativeString2Map(condition);
        return new SimpleComparativeMapChoicer(comparativeMap);
    }

    class SimpleComparativeMapChoicer implements ComparativeMapChoicer {

        private Map<String, Comparative> comparativeMap = new HashMap<String, Comparative>();

        public SimpleComparativeMapChoicer(Map<String, Comparative> comparativeMap){
            this.comparativeMap = comparativeMap;
        }

        public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
            return this.comparativeMap;
        }

        public Comparative getColumnComparative(List<Object> arguments, String colName) {
            return this.comparativeMap.get(colName);
        }
    }

}
