package com.taobao.tddl.rule;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.rule.impl.DbVirtualNodeRule;
import com.taobao.tddl.rule.impl.GroovyRule;
import com.taobao.tddl.rule.impl.TableVirtualNodeRule;
import com.taobao.tddl.rule.impl.WrappedGroovyRule;
import com.taobao.tddl.rule.utils.SimpleNamedMessageFormat;
import com.taobao.tddl.rule.virtualnode.DBTableMap;
import com.taobao.tddl.rule.virtualnode.TableSlotMap;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 对原类做一些重构，避免污染主类的可阅读性<br/>
 * 1. 抽取showTopology <br/>
 * 2. 抽取一些不常用的配置以及一些遗留的配置
 * 
 * @author jianghang 2013-11-4 下午8:04:59
 * @since 5.0.0
 */
public abstract class VirtualTableSupport extends AbstractLifecycle implements Lifecycle, VirtualTableRule {

    protected static final Logger      logger               = LoggerFactory.getLogger(TableRule.class);
    protected static final String      tableNameSepInSpring = ",";
    protected static final int         showColsPerRow       = 5;

    /** ============ 一些老的用法 ================= **/
    /**
     * 用来替换dbRules、tbRules中的占位符
     * 优先用dbRuleParames，tbRuleParames替换，其为空时再用ruleParames替换
     */
    protected String[]                 ruleParames;
    protected String[]                 dbRuleParames;
    protected String[]                 tbRuleParames;

    /** ============ 运行时变量 ================= **/
    protected DBType                   dbType               = DBType.MYSQL;                            // Oracle|MySql
    protected String                   virtualTbName;                                                  // 逻辑表名
    protected Map<String, Set<String>> actualTopology;

    // ==================== 参数处理方法 ====================
    protected void replaceWithParam(Object[] rules, String[] params) {
        if (params == null || rules == null) {
            return;
        }

        for (int i = 0; i < rules.length; i++) {
            if (rules[i] instanceof String) {
                rules[i] = replaceWithParam((String) rules[i], params);
            }
        }
    }

    private String replaceWithParam(String template, String[] params) {
        if (params == null || template == null) {
            return template;
        }
        if (params.length != 0 && params[0].indexOf(":") != -1) {
            // 只要params的第一个参数中含有冒号，就认为是NamedParam
            return replaceWithNamedParam(template, params);
        }
        return new MessageFormat(template).format(params);
    }

    /**
     * <pre>
     * 存在类似的变量配置，shardColumn:#business_id#.longValue()
     * </pre>
     * 
     * @param template
     * @param params
     * @return
     */
    private String replaceWithNamedParam(String template, String[] params) {
        Map<String, String> args = new HashMap<String, String>();
        for (String param : params) {
            int index = param.indexOf(":");
            if (index == -1) {
                throw new IllegalArgumentException("使用名字化的占位符替换失败！请检查配置。 params:" + Arrays.asList(params));
            }
            args.put(param.substring(0, index).trim(), param.substring(index + 1).trim());
        }
        return new SimpleNamedMessageFormat(template).format(args);
    }

    /**
     * @param rules 规则字符串配置
     * @param namePattern 根据isTableRule选择dbNamePattern or tbNamePattern
     * @param extraPackagesStr groovy的自定义package
     * @param dbTableMap db虚拟节点
     * @param tableSlotMap table虚拟节点
     * @param isTableRule 是否为table规则
     * @return
     */
    protected List<Rule<String>> convertToRuleArray(String[] rules, String namePattern, String extraPackagesStr,
                                                    DBTableMap dbTableMap, TableSlotMap tableSlotMap,
                                                    boolean isTableRule) {
        List<Rule<String>> ruleList = new ArrayList<Rule<String>>(1);
        if (null == rules) {// 没有rule定义
            // 一致性hash配置
            // 按照现在需求不可能为tableRule
            if (tableSlotMap != null && dbTableMap != null && !isTableRule) {
                ruleList.add(new DbVirtualNodeRule(TStringUtil.EMPTY, dbTableMap, extraPackagesStr));
                return ruleList;
            } else {
                return null;
            }
        }

        for (String rule : rules) {
            if (TStringUtil.isNotEmpty(namePattern)) {
                // 存在dbNamePattern/tbNamePattern配置
                ruleList.add(new WrappedGroovyRule(rule, namePattern, extraPackagesStr));
            } else {
                // table存在一致性hash时，必须要有rule ??
                if (tableSlotMap != null && dbTableMap != null && isTableRule) {
                    ruleList.add(new TableVirtualNodeRule(rule, tableSlotMap, extraPackagesStr));
                } else {
                    ruleList.add(new GroovyRule<String>(rule, extraPackagesStr));
                }
            }
        }

        return ruleList;
    }

    protected List<String> trimRuleString(List<String> ruleStrings) {
        List<String> result = new ArrayList<String>(ruleStrings.size());
        for (String ruleString : ruleStrings) {
            result.add(ruleString.trim());
        }
        return result;
    }

    // ==================== print topology =====================

    protected void showTopology(boolean showMap) {
        int crossIndex, endIndex, maxcolsPerRow = showColsPerRow, maxtbnlen = 1, maxdbnlen = 1;
        for (Map.Entry<String, Set<String>> e : this.actualTopology.entrySet()) {
            int colsPerRow = colsPerRow(e.getValue(), showColsPerRow);
            if (colsPerRow > maxcolsPerRow) {
                maxcolsPerRow = colsPerRow;
            }
            if (e.getKey().length() > maxdbnlen) {
                maxdbnlen = e.getKey().length(); // dbIndex最大长度
            }
            for (String tbn : e.getValue()) {
                if (tbn.length() > maxtbnlen) {
                    maxtbnlen = tbn.length(); // tableName最大长度
                }
            }
        }
        crossIndex = maxdbnlen + 1;
        endIndex = crossIndex + (maxtbnlen + 1) * maxcolsPerRow + 1;
        StringBuilder sb = new StringBuilder("The topology of the virtual table " + this.virtualTbName);
        addLine(sb, crossIndex, endIndex);
        for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology.entrySet()) {
            sb.append("\n|");
            sb.append(fillAfter(e.getKey(), maxdbnlen)).append("|");
            int i = 0, n = e.getValue().size();
            for (String tb : e.getValue()) {
                sb.append(fillAfter(tb, maxtbnlen)).append(",");
                i++;
                if (i % maxcolsPerRow == 0 && i < n) {
                    sb.append("|\n|").append(fillAfter(" ", maxdbnlen)).append("|");// 折行后把库列输出
                }
            }
            if (i % maxcolsPerRow != 0) {
                int taillen = (maxcolsPerRow - (i % maxcolsPerRow)) * (maxtbnlen + 1) + 1;
                sb.append(fillBefore("|", taillen));
            } else {
                sb.append("|");
            }

            addLine(sb, crossIndex, endIndex);
        }
        sb.append("\n");

        if (logger.isDebugEnabled()) {
            logger.debug(sb.toString());
        }

        if (!showMap) {
            return;
        }
        sb = new StringBuilder("\nYou could add below segement as the actualTopology property to ");
        sb.append(this.virtualTbName + "'s VirtualTable bean in the rule file\n\n");
        sb.append("        <property name=\"actualTopology\">\n");
        sb.append("          <map>\n");
        for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology.entrySet()) {
            sb.append("            <entry key=\"").append(e.getKey()).append("\" value=\"");
            for (String table : e.getValue()) {
                sb.append(table).append(tableNameSepInSpring);
            }
            if (sb.charAt(sb.length() - 1) == tableNameSepInSpring.charAt(0)) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("\" />\n");
        }
        sb.append("          </map>\n");
        sb.append("        </property>\n");

        if (logger.isDebugEnabled()) {
            logger.debug(sb.toString());
        }
    }

    private int colsPerRow(Collection<String> c, int maxColPerRow) {
        int n = c.size();
        if (n <= maxColPerRow) {
            return n;
        }
        int maxfiti = maxColPerRow; // 不能整除的情况下，让最后一行空白最少的那个i
        int minblank = maxColPerRow; // 不能整除的情况下，最后一行的空白个数
        for (int i = maxColPerRow; i > 0; i--) {
            int mod = n % i;
            if (mod == 0) {
                if (n / i <= i) {
                    return i; // 行数不能大于列数
                } else {
                    break;
                }
            } else {
                if (i - mod < minblank) {
                    minblank = i - mod;
                    maxfiti = i;
                }
            }
        }

        return maxfiti;
    }

    private String fillAfter(String str, int len) {
        if (str.length() < len) {
            for (int i = 0, n = len - str.length(); i < n; i++) {
                str = str + " ";
            }
        }
        return str;
    }

    private String fillBefore(String str, int len) {
        if (str.length() < len) {
            for (int i = 0, n = len - str.length(); i < n; i++) {
                str = " " + str;
            }
        }
        return str;
    }

    private void addLine(StringBuilder sb, int crossIndex, int endIndex) {
        sb.append("\n+");
        for (int i = 1; i <= endIndex; i++) {
            if (i == crossIndex || i == endIndex) {
                sb.append("+");
            } else {
                sb.append("-");
            }
        }
    }

    // ================== setter / getter ====================

    public void setRuleParames(String ruleParames) {
        if (ruleParames.indexOf('|') != -1) {
            // 优先用|线分隔,因为有些规则表达式中会有逗号
            this.ruleParames = ruleParames.split("\\|");
        } else {
            this.ruleParames = ruleParames.split(",");
        }
    }

    public void setRuleParameArray(String[] ruleParames) {
        this.ruleParames = ruleParames;
    }

    public void setDbRuleParames(String dbRuleParames) {
        this.dbRuleParames = dbRuleParames.split(",");
    }

    public void setDbRuleParameArray(String[] dbRuleParames) {
        this.dbRuleParames = dbRuleParames;
    }

    public void setTbRuleParames(String tbRuleParames) {
        this.tbRuleParames = tbRuleParames.split(",");
    }

    public void setTbRuleParameArray(String[] tbRuleParames) {
        this.tbRuleParames = tbRuleParames;
    }

    public DBType getDbType() {
        return dbType;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    public String getVirtualTbName() {
        return virtualTbName;
    }

    public void setVirtualTbName(String virtualTbName) {
        this.virtualTbName = virtualTbName;
    }

    public Map<String, Set<String>> getActualTopology() {
        return actualTopology;
    }

    public void setActualTopology(Map<String, Set<String>> actualTopology) {
        this.actualTopology = actualTopology;
    }

}
