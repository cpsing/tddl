package com.taobao.tddl.rule.impl.groovy;

import java.util.Map;

/**
 * groovy动态生成
 * 
 * @author jianghang 2013-11-4 下午3:45:10
 * @since 5.0.0
 */
public interface ShardingFunction {

    public Object eval(Map map, Object outerCtx);
}
