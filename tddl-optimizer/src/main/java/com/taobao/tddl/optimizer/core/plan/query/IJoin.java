package com.taobao.tddl.optimizer.core.plan.query;

import java.util.List;

import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * Join node of the execute plan easy to understand
 * 
 * @author Dreamond
 * @since 5.0.0
 */
public interface IJoin extends IParallelizableQueryTree<IQueryTree> {

    public enum JoinStrategy {
        HASH_JOIN, NEST_LOOP_JOIN, INDEX_NEST_LOOP, SORT_MERGE_JOIN;
    }

    /**
     * 设置join节点
     * 
     * @param left
     * @param right
     * @return
     */
    IJoin setJoinNodes(IQueryTree left, IQueryTree right);

    /**
     * 设置左join 元素
     * 
     * @param left
     * @return
     */
    IJoin setLeftNode(IQueryTree left);

    /**
     * 获取 join 元素
     * 
     * @return
     */
    IQueryTree getLeftNode();

    /**
     * 设置右join 元素
     * 
     * @param right
     * @return
     */
    IJoin setRightNode(IQueryTree right);

    /**
     * 获取左join元素
     * 
     * @return
     */
    IQueryTree getRightNode();

    /**
     * 设置join的列
     * 
     * @param leftColumns
     * @param rightColumns
     * @return
     */
    IJoin setJoinOnColumns(List<ISelectable> leftColumns, List<ISelectable> rightColumns);

    /**
     * 添加左left join的列
     * 
     * @param left
     * @return
     */
    IJoin addLeftJoinOnColumn(ISelectable left);

    /**
     * 获取左join的列
     * 
     * @return
     */
    List<ISelectable> getLeftJoinOnColumns();

    /**
     * 添加 右left join的列
     * 
     * @param right
     * @return
     */
    IJoin addRightJoinOnColumn(ISelectable right);

    /**
     * 获取右join的列名
     * 
     * @return
     */
    List<ISelectable> getRightJoinOnColumns();

    /**
     * 设置join类型
     * 
     * @param joinType
     * @return
     */
    IJoin setJoinStrategy(JoinStrategy joinStrategy);

    /**
     * 获取join类型
     * 
     * @return
     */
    JoinStrategy getJoinStrategy();

    /**
     * 是否左outer join
     * 
     * @return
     */
    Boolean getLeftOuter();

    /**
     * 是否右outer join
     * 
     * @return
     */
    Boolean getRightOuter();

    /**
     * 获取左outer join
     * 
     * @param on_off
     * @return
     */
    IJoin setLeftOuter(boolean on_off);

    /**
     * 设置右 outer join
     * 
     * @param on_off
     * @return
     */
    IJoin setRightOuter(boolean on_off);

    /**
     * 非column=column的join on中的条件
     * 
     * @return
     */
    public IFilter getOtherJoinOnFilter();

    public IJoin setOtherJoinOnFilter(IFilter otherJoinOnFilter);

    /**
     * 包含where后的所有条件，仅用于拼sql
     * 
     * @return
     */
    public IFilter getWhereFilter();

    public IJoin setWhereFilter(IFilter whereFilter);
}
