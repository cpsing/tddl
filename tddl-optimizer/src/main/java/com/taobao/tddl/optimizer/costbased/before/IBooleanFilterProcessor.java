package com.taobao.tddl.optimizer.costbased.before;

import com.taobao.tddl.optimizer.core.expression.IFilter;

/**
 * boolean filter处理器 遍历过程中，每一个boolean filter都会按照顺序调用这个Processor
 * 
 * @author Whisper
 */
public interface IBooleanFilterProcessor {

    /**
     * 处理filter，如果返回null，代表需要忽略
     */
    IFilter processBoolFilter(IFilter root);
}
