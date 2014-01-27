package com.taobao.tddl.optimizer.core.plan.query;

import java.util.List;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * @since 5.0.0
 */
public interface IMerge extends IParallelizableQueryTree<IQueryTree> {

    public List<IDataNodeExecutor> getSubNode();

    public IMerge setSubNode(List<IDataNodeExecutor> subNode);

    public IMerge addSubNode(IDataNodeExecutor subNode);

    /**
     * Merge可以根据中间结果得知具体在哪个节点上进行查询 所以Merge的分库操作可以放到执行器中进行
     * 
     * @return true 表示已经经过sharding 。 false表示未经处理
     */
    public Boolean isSharded();

    public IMerge setSharded(boolean sharded);

    public Boolean isUnion();

    public IMerge setUnion(boolean isUnion);
}
