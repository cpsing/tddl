package com.taobao.tddl.optimizer.core.ast;

/**
 * 一个抽象的公共查询树实现 可能是一个真正的queryNode,也可以是个join，或者merge实现 这是个核心的公共实现方法。
 * 
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.1.0
 */
public abstract class QueryTreeNode<RT extends QueryTreeNode> extends CanOptimizedNode<RT> {

}
