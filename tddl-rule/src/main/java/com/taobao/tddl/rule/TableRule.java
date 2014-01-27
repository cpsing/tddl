package com.taobao.tddl.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.rule.Rule.RuleColumn;
import com.taobao.tddl.rule.impl.EnumerativeRule;
import com.taobao.tddl.rule.model.AdvancedParameter;
import com.taobao.tddl.rule.utils.RuleUtils;
import com.taobao.tddl.rule.utils.sample.Samples;
import com.taobao.tddl.rule.virtualnode.DBTableMap;
import com.taobao.tddl.rule.virtualnode.TableSlotMap;

/**
 * 类名取名兼容老的rule代码<br/>
 * 描述一个逻辑表怎样分库分表，允许指定逻辑表名和db/table的{@linkplain Rule}规则
 * 
 * <pre>
 *   一个配置事例： 
 *   -----------
 *   <!-- 按照user_id取模划分64张表,表明具体为'user_0000'-'user_0063'，
 *   'user_0000'-'user_0031' 在'TDDL_0000_GROUP'中，
 *   'user_0032'-'user_0063' 在'TDDL_0001_GROUP'中 -->
 *   <bean id="user_bean" class="com.taobao.tddl.rule.VirtualTable">
 *     <!-- groupKey格式框架，{}中的数将会被dbRuleArray的值替代，并保持位数 -->
 *     <property name="dbNamePattern" value="TDDL_{0000}_GROUP"/> 
 *     <!-- 具体的分库规则 -->
 *     <property name="dbRuleArray"> 
 *       <!-- 按照user_id取模划分64张表，结果除以32后分到两个库中 -->
 *       <value>(#user_id,1,64#.longValue() % 64).intdiv(32)</value> 
 *     </property>
 *     <!-- 具体表名格式框架，{}中的数将会被tbRuleArray的值替代，并保持位数 -->
 *     <property name="tbNamePattern" value="user_{0000}"></property> 
 *     <!-- 具体的分表规则 -->
 *     <property name="tbRuleArray"> 
 *       <!-- 按照user_id取模划分64张表 -->
 *       <value>#user_id,1,64#.longValue() % 64</value> 
 *     </property>
 *     <!-- 全表扫描开关，默认关闭，是否允许应用端在没有给定分表键值的情况下查询全表 -->
 *     <property name="allowFullTableScan" value="true"/> 
 *   </bean>
 * </pre>
 * 
 * @author linxuan
 * @author jianghang 2013-11-4 下午5:33:51
 * @since 5.0.0
 */
public class TableRule extends VirtualTableSupport implements VirtualTableRule {

    /** =================================================== **/
    /** == 原始的配置字符串 == **/
    /** =================================================== **/
    protected String             dbNamePattern;               // item_{0000}_dbkey
    protected String             tbNamePattern;               // item_{0000}
    protected String[]           dbRules;                     // rule配置字符串
    protected String[]           tbRules;                     // rule配置字符串
    protected List<String>       extraPackages;               // 自定义用户包

    protected boolean            allowReverseOutput;          // 是否允许反向输出
    protected boolean            allowFullTableScan   = false; // 是否允许全表扫描
    protected boolean            disableFullTableScan = true; // 是否关闭全表扫描

    /**
     * 虚拟节点映射
     */
    protected TableSlotMap       tableSlotMap;
    protected DBTableMap         dbTableMap;
    protected String             tableSlotKeyFormat;

    /** ============ 运行时变量 ================= **/

    protected List<Rule<String>> dbShardRules;
    protected List<Rule<String>> tbShardRules;
    protected List<String>       shardColumns;                // 分库字段
    protected Object             outerContext;

    /**
     * 是否是个广播表，optimizer模块需要用他来标记某个表是否需要进行复制
     */
    protected boolean            broadcast            = false;

    /**
     * 相同的join group 应该具有相同的切分规则
     */
    protected String             joinGroup            = null;

    public void doInit() {
        // 初始化虚拟节点
        if (tableSlotMap != null) {
            tableSlotMap.setTableSlotKeyFormat(tableSlotKeyFormat);
            tableSlotMap.setLogicTable(this.virtualTbName);
            tableSlotMap.init();
        }

        if (dbTableMap != null) {
            dbTableMap.setTableSlotKeyFormat(tableSlotKeyFormat);
            dbTableMap.setLogicTable(this.virtualTbName);
            dbTableMap.init();
        }

        // 处理一下占位符
        replaceWithParam(this.dbRules, dbRuleParames != null ? dbRuleParames : ruleParames);
        replaceWithParam(this.tbRules, tbRuleParames != null ? tbRuleParames : ruleParames);

        // 构造一下Rule对象
        String packagesStr = buildExtraPackagesStr(extraPackages);
        if (dbShardRules == null) { // 如果未设置Rule对象，基于string生成rule对象
            setDbShardRules(convertToRuleArray(dbRules, dbNamePattern, packagesStr, dbTableMap, tableSlotMap, false));
        }
        if (tbShardRules == null) {
            setTbShardRules(convertToRuleArray(tbRules, tbNamePattern, packagesStr, dbTableMap, tableSlotMap, true));
        }

        if (tbShardRules == null || tbShardRules.size() == 0) {
            if (this.tbNamePattern == null) {
                // 表规则没有，tbKeyPattern为空
                this.tbNamePattern = this.virtualTbName;
            }
        }

        // 构造一下分区字段
        buildShardColumns();

        // 基于rule计算一下拓扑结构
        initActualTopology();
    }

    public void initActualTopology() {
        if (actualTopology != null) {
            // 将逗号分隔的表名转换为Set
            for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology.entrySet()) {
                if (e.getValue().size() == 1) { // 如果只有一个元素，则怀疑为逗号分隔的
                    Set<String> tables = new LinkedHashSet<String>();
                    tables.addAll(Arrays.asList(e.getValue().iterator().next().split(tableNameSepInSpring)));
                    e.setValue(tables);
                }
            }
            showTopology(false);
            return; // 用户显式设置的优先
        }

        actualTopology = new TreeMap<String, Set<String>>();
        if (RuleUtils.isEmpty(dbShardRules) && RuleUtils.isEmpty(tbShardRules)) { // 啥规则都没有
            Set<String> tbs = new TreeSet<String>();
            tbs.add(this.tbNamePattern);
            actualTopology.put(this.dbNamePattern, tbs);
        } else if (RuleUtils.isEmpty(dbShardRules)) { // 没有库规则
            Set<String> tbs = new TreeSet<String>();
            for (Rule<String> tbRule : tbShardRules) {
                tbs.addAll(vbvRule(tbRule, getEnumerates(tbRule)));
            }
            actualTopology.put(this.dbNamePattern, tbs);
        } else if (RuleUtils.isEmpty(tbShardRules)) {// 没有表规则
            Set<String> tbs = new TreeSet<String>();
            tbs.add(this.tbNamePattern);
            for (Rule<String> dbRule : dbShardRules) {
                for (String dbIndex : vbvRule(dbRule, getEnumerates(dbRule))) {
                    actualTopology.put(dbIndex, tbs);
                }
            }
        } else { // 库表规则都有
            for (Rule<String> dbRule : dbShardRules) {
                for (Rule<String> tbRule : tbShardRules) {
                    if (this.tableSlotMap != null && this.dbTableMap != null) { // 存在vnode
                        valueByValue(this.actualTopology, dbRule, tbRule, true);
                    } else {
                        valueByValue(this.actualTopology, dbRule, tbRule, false);
                    }
                }
            }
        }

        // 打印一下
        showTopology(true);
    }

    // ==================== build topology =====================

    // 计算一下规则
    private Set<String> vbvRule(Rule<String> rule, Samples samples) {
        Set<String> tbs = new TreeSet<String>();
        for (Map<String, Object> sample : samples) {
            tbs.add(rule.eval(sample, null));
        }
        return tbs;
    }

    // 计算一下规则
    private Set<String> vbvRule(Rule<String> rule, Map<String, Set<Object>> enumerates) {
        Set<String> tbs = new TreeSet<String>();
        for (Map<String, Object> sample : new Samples(enumerates)) {
            tbs.add(rule.eval(sample, null));
        }
        return tbs;
    }

    // 计算一下规则，同时记录对应计算参数
    private Map<String, Samples> vbvTrace(Rule<String> rule, Map<String, Set<Object>> enumerates) {
        Map<String, Samples> db2Samples = new TreeMap<String, Samples>();
        Samples dbSamples = new Samples(enumerates);
        for (Map<String, Object> sample : dbSamples) {
            String v = rule.eval(sample, null);
            Samples s = db2Samples.get(v);
            if (s == null) {
                s = new Samples(sample.keySet());
                db2Samples.put(v, s);
            }
            s.addSample(sample);
        }
        return db2Samples;
    }

    private void valueByValue(Map<String, Set<String>> topology, Rule<String> dbRule, Rule<String> tbRule,
                              boolean isVnode) {
        Map<String/* 列名 */, Set<Object>> dbEnumerates = getEnumerates(dbRule);
        Map<String/* 列名 */, Set<Object>> tbEnumerates = getEnumerates(tbRule);

        // 以table rule列为基准
        Set<AdvancedParameter> params = RuleUtils.cast(tbRule.getRuleColumnSet());
        for (AdvancedParameter tbap : params) {
            if (dbEnumerates.containsKey(tbap.key)) {
                // 库表规则的公共列名，表枚举值要涵盖所有库枚举值跨越的范围= =!
                Set<Object> tbValuesBasedONdbValue = new HashSet<Object>();
                for (Object dbValue : dbEnumerates.get(tbap.key)) {
                    tbValuesBasedONdbValue.addAll(tbap.enumerateRange(dbValue));
                }
                dbEnumerates.get(tbap.key).addAll(tbValuesBasedONdbValue);
            } else {
                dbEnumerates.put(tbap.key, tbEnumerates.get(tbap.key));
            }
        }

        // 有虚拟节点的话按照虚拟节点计算
        if (isVnode) {
            Samples tabSamples = new Samples(tbEnumerates);
            Set<String> tbs = new TreeSet<String>();
            for (Map<String, Object> sample : tabSamples) {
                String value = tbRule.eval(sample, null);
                tbs.add(value);
            }

            for (String table : tbs) {
                Map<String, Object> sample = new HashMap<String, Object>(1);
                sample.put(EnumerativeRule.REAL_TABLE_NAME_KEY, table);
                String db = dbRule.eval(sample, null);
                if (topology.get(db) == null) {
                    Set<String> tabs = new HashSet<String>();
                    tabs.add(table);
                    topology.put(db, tabs);
                } else {
                    topology.get(db).add(table);
                }
            }

            return;
        } else {
            // 没有虚拟节点按正常走
            Map<String, Samples> dbs = vbvTrace(dbRule, dbEnumerates);// 库计算结果，与得到结果的输入值集合
            for (Map.Entry<String/* 库值 */, Samples> e : dbs.entrySet()) {
                Set<String> tbs = topology.get(e.getKey());
                if (tbs == null) {
                    tbs = vbvRule(tbRule, e.getValue());
                    topology.put(e.getKey(), tbs);
                } else {
                    tbs.addAll(vbvRule(tbRule, e.getValue()));
                }
            }
        }
    }

    /**
     * 根据#id,1,32|512_64#中第三段定义的遍历值范围，枚举规则中每个列的遍历值
     */
    private Map<String/* 列名 */, Set<Object>/* 列值 */> getEnumerates(Rule rule) {
        Set<AdvancedParameter> params = RuleUtils.cast(rule.getRuleColumnSet());
        Map<String/* 列名 */, Set<Object>/* 列值 */> enumerates = new HashMap<String, Set<Object>>(params.size());
        for (AdvancedParameter ap : params) {
            enumerates.put(ap.key, ap.enumerateRange());
        }
        return enumerates;
    }

    private String buildExtraPackagesStr(List<String> extraPackages) {
        StringBuilder ep = new StringBuilder("");
        if (extraPackages != null) {
            int packNum = extraPackages.size();
            for (int i = 0; i < packNum; i++) {
                ep.append("import ");
                ep.append(extraPackages.get(i));
                ep.append(";");
            }
        }
        return ep.toString();
    }

    private void buildShardColumns() {
        Set<String> shardColumns = new HashSet<String>();
        if (null != dbShardRules) {
            for (Rule<String> rule : dbShardRules) {
                Map<String, RuleColumn> columRule = rule.getRuleColumns();
                if (null != columRule && !columRule.isEmpty()) {
                    shardColumns.addAll(columRule.keySet());
                }
            }
        }

        if (null != tbShardRules) {
            for (Rule<String> rule : tbShardRules) {
                Map<String, RuleColumn> columRule = rule.getRuleColumns();
                if (null != columRule && !columRule.isEmpty()) {
                    shardColumns.addAll(columRule.keySet());
                }
            }
        }

        this.shardColumns = new ArrayList<String>(shardColumns);// 没有表配置，代表没有走分区
    }

    // ===================== setter / getter ====================

    public void setDbRuleArray(List<String> dbRules) {
        // 若类型改为String[],spring会自动以逗号分隔，变态！
        dbRules = trimRuleString(dbRules);
        this.dbRules = dbRules.toArray(new String[dbRules.size()]);
    }

    public void setTbRuleArray(List<String> tbRules) {
        // 若类型改为String[],spring会自动以逗号分隔，变态！
        tbRules = trimRuleString(tbRules);
        this.tbRules = tbRules.toArray(new String[tbRules.size()]);
    }

    public void setDbRules(String dbRules) {
        if (this.dbRules == null) {
            // 优先级比dbRuleArray低
            // this.dbRules = dbRules.split("\\|");
            this.dbRules = new String[] { dbRules.trim() }; // 废掉|分隔符，没人用且容易造成混乱
        }
    }

    public void setTbRules(String tbRules) {
        if (this.tbRules == null) {
            // 优先级比tbRuleArray低
            // this.tbRules = tbRules.split("\\|");
            this.tbRules = new String[] { tbRules.trim() }; // 废掉|分隔符，没人用且容易造成混乱
        }
    }

    public void setRuleParames(String ruleParames) {
        if (ruleParames.indexOf('|') != -1) {
            // 优先用|线分隔,因为有些规则表达式中会有逗号
            this.ruleParames = ruleParames.split("\\|");
        } else {
            this.ruleParames = ruleParames.split(",");
        }
    }

    public void setDbNamePattern(String dbKeyPattern) {
        this.dbNamePattern = dbKeyPattern;
    }

    public void setTbNamePattern(String tbKeyPattern) {
        this.tbNamePattern = tbKeyPattern;
    }

    public void setExtraPackages(List<String> extraPackages) {
        this.extraPackages = extraPackages;
    }

    public void setOuterContext(Object outerContext) {
        this.outerContext = outerContext;
    }

    public void setAllowReverseOutput(boolean allowReverseOutput) {
        this.allowReverseOutput = allowReverseOutput;
    }

    public boolean isDisableFullTableScan() {
        return disableFullTableScan;
    }

    public void setDisableFullTableScan(boolean disableFullTableScan) {
        this.disableFullTableScan = disableFullTableScan;
    }

    public void setAllowFullTableScan(boolean allowFullTableScan) {
        this.allowFullTableScan = allowFullTableScan;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    public void setDbShardRules(List<Rule<String>> dbShardRules) {
        this.dbShardRules = dbShardRules;
    }

    public void setTbShardRules(List<Rule<String>> tbShardRules) {
        this.tbShardRules = tbShardRules;
    }

    public DBType getDbType() {
        return dbType;
    }

    public List getDbShardRules() {
        return dbShardRules;
    }

    public List getTbShardRules() {
        return tbShardRules;
    }

    public Object getOuterContext() {
        return outerContext;
    }

    public TableSlotMap getTableSlotMap() {
        return tableSlotMap;
    }

    public DBTableMap getDbTableMap() {
        return dbTableMap;
    }

    public boolean isAllowReverseOutput() {
        return allowReverseOutput;
    }

    public boolean isAllowFullTableScan() {
        return allowFullTableScan;
    }

    public String getTbNamePattern() {
        return tbNamePattern;
    }

    public String getDbNamePattern() {
        return dbNamePattern;
    }

    public String[] getDbRuleStrs() {
        return dbRules;
    }

    public String[] getTbRulesStrs() {
        return tbRules;
    }

    public void setDbTableMap(DBTableMap dbTableMap) {
        this.dbTableMap = dbTableMap;
    }

    public void setTableSlotMap(TableSlotMap tableSlotMap) {
        this.tableSlotMap = tableSlotMap;
    }

    public void setTableSlotKeyFormat(String tableSlotKeyFormat) {
        this.tableSlotKeyFormat = tableSlotKeyFormat;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    public String getJoinGroup() {
        return joinGroup;
    }

    public void setJoinGroup(String joinGroup) {
        this.joinGroup = joinGroup;
    }

    public List<String> getShardColumns() {
        return shardColumns;
    }
}
