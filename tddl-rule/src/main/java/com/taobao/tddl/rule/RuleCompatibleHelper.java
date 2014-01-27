package com.taobao.tddl.rule;

import org.apache.commons.lang.StringUtils;

/**
 * 做一下老的rule兼容处理，tddl5版本类名做了下统一调整，所以以前配置中使用的一下类名已经不存在，需要做转换
 * 
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @since 5.0.0
 */
public class RuleCompatibleHelper {

    // 当前使用的类名
    public static final String RULE_CURRENT_ROOT_NAME   = "com.taobao.tddl.rule.VirtualTableRoot";
    public static final String RULE_CURRENT_TABLE_NAME  = "com.taobao.tddl.rule.TableRule";

    // 历史曾经使用过的类名
    public static final String RULE_INTERACT_ROOT_NAME  = "com.taobao.tddl.interact.rule.VirtualTableRoot";
    public static final String RULE_INTERACT_TABLE_NAME = "com.taobao.tddl.interact.rule.TableRule";

    public static final String RULE_LE_ROOT_NAME        = "com.taobao.tddl.rule.le.VirtualTableRoot";
    public static final String RULE_LE_TABLE_NAME       = "com.taobao.tddl.rule.le.TableRule";

    public static final String RULE_LE2_ROOT_NAME       = "com.taobao.tddl.rule.VirtualTableRoot";
    public static final String RULE_LE2_TABLE_NAME      = "com.taobao.tddl.rule.config.TableRule";

    public static String compatibleRule(String ruleStr) {
        ruleStr = StringUtils.replace(ruleStr, RULE_INTERACT_ROOT_NAME, RULE_CURRENT_ROOT_NAME);
        ruleStr = StringUtils.replace(ruleStr, RULE_INTERACT_TABLE_NAME, RULE_CURRENT_TABLE_NAME);

        ruleStr = StringUtils.replace(ruleStr, RULE_LE_ROOT_NAME, RULE_CURRENT_ROOT_NAME);
        ruleStr = StringUtils.replace(ruleStr, RULE_LE_TABLE_NAME, RULE_CURRENT_TABLE_NAME);

        // ruleStr = StringUtils.replace(ruleStr, RULE_LE2_ROOT_NAME,
        // RULE_CURRENT_ROOT_NAME);
        ruleStr = StringUtils.replace(ruleStr, RULE_LE2_TABLE_NAME, RULE_CURRENT_TABLE_NAME);
        return ruleStr;
    }
}
