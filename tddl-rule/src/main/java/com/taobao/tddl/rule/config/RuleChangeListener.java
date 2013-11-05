package com.taobao.tddl.rule.config;

/**
 * 允许业务监听rule变化的通知
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-5-4 12:01:06
 */
public interface RuleChangeListener {

    public void onRuleRecieve(String oldRuleStr);
}
