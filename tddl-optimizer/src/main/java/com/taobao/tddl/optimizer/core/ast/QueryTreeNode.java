package com.taobao.tddl.optimizer.core.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.LOCK_MODEL;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 一个抽象的公共查询树实现 可能是一个真正的queryNode,也可以是个join，或者merge实现 这是个核心的公共实现方法。
 * 
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.0.0
 */
public abstract class QueryTreeNode extends ASTNode<QueryTreeNode> {

    public static enum FilterType {
        /**
         * 如果有索引，就是索引上的keyFilter，如果没索引，就是主表上的KeyFilter
         */
        IndexQueryKeyFilter,
        /**
         * 在主表上的ValueFilter
         */
        ResultFilter,

        /**
         * 在索引表上的ValueFilter
         */
        IndexQueryValueFilter;
    }

    protected IFilter           resultFilter;
    protected IFilter           keyFilter;

    /**
     * 包含所有子节点的filter，用于拼sql
     */
    protected IFilter           allWhereFilter   = null;

    /**
     * select查询中的列
     */
    protected List<ISelectable> columnsSelected  = new ArrayList<ISelectable>();

    /**
     * 依赖的所有列，可以理解为columnsSelected + implicitSelectable的总合
     */
    protected List<ISelectable> columnsRefered   = new ArrayList<ISelectable>();

    /**
     * 显式的由查询接口指定的orderBy，注意需要保证顺序
     */
    protected List<IOrderBy>    orderBys         = new LinkedList<IOrderBy>();

    /**
     * 显式的由查询接口指定的group by，注意需要保证顺序
     */
    protected List<IOrderBy>    groups           = new LinkedList<IOrderBy>();

    /**
     * having条件
     */
    protected IFilter           havingFilter;

    /**
     * 上一层父节点，比如子查询会依赖父节点的字段信息
     * http://dev.mysql.com/doc/refman/5.0/en/correlated-subqueries.html
     */
    protected ASTNode           parent           = null;

    /**
     * join的子节点
     */
    protected List<ASTNode>     children         = new ArrayList<ASTNode>(2);

    /**
     * 从哪里开始
     */
    protected Comparable        limitFrom        = null;

    /**
     * 到哪里结束
     */
    protected Comparable        limitTo          = null;

    /**
     * filter in where
     */
    protected IFilter           whereFilter      = null;

    /**
     * 非column=column的join列
     */
    protected IFilter           otherJoinOnFilter;

    /**
     * 当前qn的别名，用于进行join等操作的时候辨别到底这一行是从哪个subNode来的。
     */
    protected String            alias;

    /**
     * 如果出现subQuery，内外都存在别名时，内部的别名为subAlias，外部使用的别名为alias
     */
    protected String            subAlias;

    protected boolean           consistent       = true;

    protected LOCK_MODEL        lockModel        = LOCK_MODEL.SHARED_LOCK;

    /**
     * 当前节点是否为子查询
     */
    protected boolean           subQuery         = false;
    protected boolean           needBuild        = true;
    protected QUERY_CONCURRENCY queryConcurrency;

    /**
     * 是否为存在聚合信息，比如出现limit/group by/count/max等，此节点就会被标记为true，不允许进行join merge
     * join的展开优化
     */
    protected boolean           isExistAggregate = false;

    /**
     * 获取完整的order by列信息(包括隐藏，比如group by会转化为order by)
     */
    public abstract List<IOrderBy> getImplicitOrderBys();

    @Override
    public abstract IQueryTree toDataNodeExecutor() throws QueryException;

    /**
     * 获取builder对象
     */
    public abstract QueryTreeNodeBuilder getBuilder();

    public abstract String getName();

    /**
     * 需要子类实现各自的转换
     */
    public QueryTreeNode convertToJoinIfNeed() {
        for (int i = 0; i < this.getChildren().size(); i++) {
            if (this.getChildren().get(i) instanceof QueryTreeNode) {
                this.getChildren().set(i, ((QueryTreeNode) this.getChildren().get(i)).convertToJoinIfNeed());
            }
        }

        return this;
    }

    @Override
    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        for (ASTNode child : this.getChildren()) {
            child.assignment(parameterSettings);
        }

        this.keyFilter = OptimizerUtils.assignment(this.keyFilter, parameterSettings);
        this.whereFilter = OptimizerUtils.assignment(this.whereFilter, parameterSettings);
        this.allWhereFilter = OptimizerUtils.assignment(this.allWhereFilter, parameterSettings);
        this.havingFilter = OptimizerUtils.assignment(this.havingFilter, parameterSettings);
        this.resultFilter = OptimizerUtils.assignment(this.resultFilter, parameterSettings);
        this.columnsSelected = OptimizerUtils.assignment(this.columnsSelected, parameterSettings);
        this.otherJoinOnFilter = OptimizerUtils.assignment(this.otherJoinOnFilter, parameterSettings);
        if (this.limitFrom instanceof IBindVal) {
            Object value = ((IBindVal) limitFrom).assignment(parameterSettings);
            limitFrom = (Comparable) OptimizerUtils.convertType(value, DataType.LongType);
        }

        if (this.limitTo instanceof IBindVal) {
            Object value = ((IBindVal) limitTo).assignment(parameterSettings);
            limitTo = (Comparable) OptimizerUtils.convertType(value, DataType.LongType);
        }
    }

    // =================== build / copy =========================

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

    @Override
    public String toString() {
        return this.toString(0);
    }

    // ======================= setter / getter ======================

    public Comparable getLimitFrom() {
        return limitFrom;
    }

    public QueryTreeNode setLimitFrom(Comparable limitFrom) {
        if (limitFrom != null && !(limitFrom instanceof IBindVal)) {
            this.limitFrom = Long.valueOf(limitFrom.toString());
        }

        this.limitFrom = limitFrom;
        return this;
    }

    public Comparable getLimitTo() {
        return limitTo;
    }

    public QueryTreeNode setLimitTo(Comparable limitTo) {
        if (limitTo != null && !(limitTo instanceof IBindVal)) {
            this.limitTo = Long.valueOf(limitTo.toString());
        }

        this.limitTo = limitTo;
        return this;
    }

    public QueryTreeNode limit(long i, long j) {
        this.setLimitFrom(i);
        this.setLimitTo(j);
        return this;
    }

    public IFilter getWhereFilter() {
        return whereFilter;
    }

    public QueryTreeNode query(IFilter whereFilter) {
        this.whereFilter = whereFilter;
        setNeedBuild(true);
        return this;
    }

    public QueryTreeNode query(String where) {
        this.whereFilter = FilterUtils.createFilter(where);
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public QueryTreeNode alias(String alias) {
        this.alias = alias;
        setNeedBuild(true);
        return this;
    }

    public QueryTreeNode subAlias(String alias) {
        this.subAlias = alias;
        setNeedBuild(true);
        return this;
    }

    public List<IOrderBy> getGroupBys() {
        return this.groups;
    }

    public QueryTreeNode setGroupBys(List<IOrderBy> groups) {
        this.groups = groups;
        setNeedBuild(true);
        return this;
    }

    public List<IOrderBy> getOrderBys() {
        return orderBys;
    }

    public void setOrderBys(List<IOrderBy> orderBys) {
        this.orderBys = orderBys;
        setNeedBuild(true);
    }

    public IFilter getResultFilter() {
        return this.resultFilter;
    }

    public void setResultFilter(IFilter f) {
        this.resultFilter = f;
        setNeedBuild(true);
    }

    public void addResultFilter(IBooleanFilter filter) {
        if (this.resultFilter == null) {
            this.resultFilter = filter;
            return;
        }

        if (this.resultFilter instanceof ILogicalFilter) {
            // 添加为子节点
            ((ILogicalFilter) resultFilter).addSubFilter(filter);
            setNeedBuild(true);
        } else {
            // 将自己做为一个Logical filter的子条件
            IFilter sub = this.resultFilter;
            resultFilter = ASTNodeFactory.getInstance().createLogicalFilter();
            resultFilter.setOperation(OPERATION.AND);
            ((ILogicalFilter) resultFilter).addSubFilter(sub);
        }
    }

    public IFilter getKeyFilter() {
        return this.keyFilter;
    }

    public void setKeyFilter(IFilter f) {
        this.keyFilter = f;
        setNeedBuild(true);
    }

    public List<ASTNode> getChildren() {
        return this.children;
    }

    public ASTNode getChild() {
        if (this.getChildren().isEmpty()) {
            return null;
        }
        return this.getChildren().get(0);
    }

    public void addChild(ASTNode childNode) {
        this.children.add(childNode);
    }

    public QueryTreeNode parent(QueryTreeNode qtn) {
        this.parent = qtn;
        return this;
    }

    public List<ISelectable> getColumnsSelected() {
        return columnsSelected;
    }

    public QueryTreeNode setColumnsSelected(List<ISelectable> columnsSelected) {
        this.columnsSelected = columnsSelected;
        return this;
    }

    /**
     * 添加一个不存在的字段
     */
    public void addColumnsSelected(ISelectable selected) {
        if (!columnsSelected.contains(selected)) {
            columnsSelected.add(selected);
        }
    }

    /**
     * 添加一个不存在的字段
     */
    public void addColumnsRefered(ISelectable selected) {
        if (!columnsRefered.contains(selected)) {
            columnsRefered.add(selected);
        }
    }

    /**
     * 判断一个字段是否存在于当前库
     */
    public boolean hasColumn(ISelectable c) {
        return this.getBuilder().findColumn(c) != null;
    }

    /**
     * 根据字段获取一下字段，查询的字段可能来自于select或者from
     */
    public ISelectable findColumn(ISelectable c) {
        return this.getBuilder().findColumn(c);
    }

    public QueryTreeNode select(List<ISelectable> columnSelected) {
        this.columnsSelected = columnSelected;
        setNeedBuild(true);
        return this;
    }

    public QueryTreeNode select(ISelectable... columnSelected) {
        this.select(new ArrayList<ISelectable>(Arrays.asList(columnSelected)));
        return this;
    }

    public List<ISelectable> getColumnsRefered() {
        return this.columnsRefered;
    }

    public void setColumnsRefered(List<ISelectable> columnsRefered) {
        this.columnsRefered = columnsRefered;
    }

    /**
     * 列的tablename会设为表别名
     * 
     * @return
     */
    public List<ISelectable> getColumnsSelectedForParent() {
        List<ISelectable> res = new ArrayList<ISelectable>(this.getColumnsSelected().size());
        for (ISelectable s : this.getColumnsSelected()) {
            ISelectable a = s.copy();
            if (this.getAlias() != null) {
                a.setTableName(this.getAlias());
            }

            if (s.getAlias() != null) {
                a.setColumnName(s.getAlias()); // 设置为alias name
                a.setAlias(null);
            }
            res.add(a);
        }
        return res;
    }

    /**
     * 列的tablename会设为表别名
     * 
     * @return
     */
    public List<ISelectable> getColumnsReferedForParent() {
        if (this.getAlias() == null) {
            return this.getColumnsRefered();
        } else {
            List<ISelectable> res = new ArrayList<ISelectable>(this.getColumnsRefered().size());
            for (ISelectable s : this.getColumnsRefered()) {
                ISelectable a = s.copy();
                if (this.getAlias() != null) {
                    a.setTableName(this.getAlias());
                }

                if (s.getAlias() != null) {
                    a.setColumnName(s.getAlias()); // 设置为alias name
                    a.setAlias(null);
                }
                res.add(a);
            }
            return res;
        }
    }

    /**
     * 列的tablename会设为表别名
     * 
     * @return
     */
    public boolean containsColumnsReferedForParent(ISelectable c) {
        if (c instanceof IBooleanFilter) {
            if (((IBooleanFilter) c).getOperation().equals(OPERATION.CONSTANT)) {
                return true;
            }
        }

        return this.getColumnsReferedForParent().contains(c);
    }

    /**
     * join 其他节点
     * 
     * @param o
     * @return
     */
    public JoinNode join(QueryTreeNode o) {
        JoinNode joinNode = new JoinNode();
        joinNode.addChild(this);
        joinNode.addChild(o);
        return joinNode;
    }

    public JoinNode join(String rightNode) {
        return this.join(new TableNode(rightNode));
    }

    public JoinNode join(String rightTable, String rightFilter, String leftKey, String rightKey) {
        return this.join(new TableNode(rightTable).query(rightFilter)).addJoinKeys(leftKey, rightKey);
    }

    public JoinNode join(String rightTable, String rightFilter) {
        return this.join(new TableNode(rightTable).query(rightFilter));
    }

    public JoinNode join(String rightTable, String leftKey, String rightKey) {
        return this.join(new TableNode(rightTable)).addJoinKeys(leftKey, rightKey);
    }

    public JoinNode join(QueryTreeNode rightNode, String leftKey, String rightKey) {
        return this.join(rightNode).addJoinKeys(leftKey, rightKey);
    }

    public MergeNode merge(ASTNode o) {
        MergeNode m = new MergeNode();
        m.addChild(this);
        m.addChild(o);
        return m;
    }

    public MergeNode merge(List<ASTNode> os) {
        MergeNode m = new MergeNode();
        m.addChild(this);
        m.getChildren().addAll(os);
        return m;
    }

    public QueryTreeNode groupBy(ISelectable c, boolean b) {
        IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy();
        orderBy.setColumn(c);
        orderBy.setDirection(b);

        this.groups.add(orderBy);
        setNeedBuild(true);
        return this;
    }

    public QueryTreeNode groupBy(ISelectable c) {
        IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy();
        orderBy.setColumn(c);

        setNeedBuild(true);
        this.groups.add(orderBy);
        return this;
    }

    public QueryTreeNode groupBy(String group) {
        return this.groupBy(OptimizerUtils.createColumnFromString(group));
    }

    public QueryTreeNode groupBy(IOrderBy sc) {
        setNeedBuild(true);
        this.groups.add(sc);
        return this;
    }

    public QueryTreeNode orderBy(ISelectable c, boolean asc) {
        IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy();
        orderBy.setColumn(c);
        orderBy.setDirection(asc);

        if (!this.orderBys.contains(orderBy)) {
            this.orderBys.add(orderBy);
            setNeedBuild(true);
        }
        return this;
    }

    public QueryTreeNode orderBy(String o) {
        return orderBy(o, true);
    }

    public QueryTreeNode orderBy(String o, boolean asc) {
        IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy();
        String column = o;
        orderBy.setDirection(asc);
        return this.orderBy(OptimizerUtils.createColumnFromString(column), orderBy.getDirection());
    }

    /**
     * 多个列时，允许逗号分隔
     */
    public QueryTreeNode select(String s) {
        s = s.toUpperCase();
        List<ISelectable> ss = new ArrayList();
        for (String cn : s.split(",")) {
            ss.add(OptimizerUtils.createColumnFromString(cn));
        }
        return this.select(ss);
    }

    public QueryTreeNode query(String where, String select) {
        return this.query(where).select(select);
    }

    /**
     * 设置别名，表级别
     */
    public QueryTreeNode setAlias(String string) {
        this.alias(string);
        return this;
    }

    /**
     * 设置别名，表级别
     */
    public QueryTreeNode setSubAlias(String string) {
        this.subAlias(string);
        return this;
    }

    public String getSubAlias() {
        return subAlias;
    }

    public boolean isSubQuery() {
        return subQuery;
    }

    public QueryTreeNode setSubQuery(boolean subQuery) {
        setNeedBuild(true);
        this.subQuery = subQuery;
        return this;
    }

    @Override
    public boolean isNeedBuild() {
        if (this.needBuild) {
            return true;
        }

        // 如果子查询发生了改变，也需要重新build
        for (ASTNode child : this.getChildren()) {
            if (child.isNeedBuild()) {
                needBuild = true;
                return true;
            }
        }

        return false;
    }

    public QueryTreeNode setConsistent(boolean b) {
        this.consistent = b;
        return this;
    }

    public boolean getConsistent() {
        return this.consistent;
    }

    public QUERY_CONCURRENCY getQueryConcurrency() {
        return this.queryConcurrency;
    }

    public QueryTreeNode setQueryConcurrency(QUERY_CONCURRENCY qc) {
        this.queryConcurrency = qc;
        return this;
    }

    public QueryTreeNode having(IFilter havingFilter) {
        this.havingFilter = havingFilter;
        return this;
    }

    public QueryTreeNode having(String having) {
        this.havingFilter = FilterUtils.createFilter(having);
        return this;
    }

    public IFilter getHavingFilter() {
        return this.havingFilter;
    }

    public boolean containsKeyFilter() {
        return this.keyFilter != null;
    }

    public IFilter getOtherJoinOnFilter() {
        return otherJoinOnFilter;
    }

    public QueryTreeNode setOtherJoinOnFilter(IFilter otherJoinOnFilter) {
        this.otherJoinOnFilter = otherJoinOnFilter;
        return this;
    }

    public IFilter getAllWhereFilter() {
        return allWhereFilter;
    }

    public QueryTreeNode setAllWhereFilter(IFilter allWhereFilter) {
        this.allWhereFilter = allWhereFilter;
        return this;
    }

    public LOCK_MODEL getLockModel() {
        return lockModel;
    }

    public boolean isExistAggregate() {
        return isExistAggregate;
    }

    public QueryTreeNode setExistAggregate(boolean isExistAggregate) {
        this.isExistAggregate = isExistAggregate;
        return this;
    }

    // ==================== helper method =================

    /**
     * <pre>
     * distinct和group by的字段顺序打乱的计算结果是等价的，所以可以对此做一些优化
     * 
     * 1. order by和group by同时存在时，并且group by包含了order by所有的字段，调整groupBy的字段顺序符合order by的顺序，多余的字段并下推到order by
     * 如: group by c1 c2 c3 order by c2 c1，可优化为：group by c2 c1 c3 order by c2 c1 c3，同时可将order by下推
     * 2. 只存在group by，复制group by字段到order by
     * </pre>
     */
    public List<IOrderBy> getOrderByCombineWithGroupBy() {
        // 如果用户没指定order by，则显示index的order by
        if (this.getOrderBys() != null && !this.getOrderBys().isEmpty()) {
            if (this.getGroupBys() != null && !this.getGroupBys().isEmpty()) {
                List<IOrderBy> newOrderBys = new ArrayList<IOrderBy>();
                // 首先以order by的顺序，查找group by中对应的字段
                for (IOrderBy orderBy : this.getOrderBys()) {
                    if (findOrderByByColumn(this.getGroupBys(), orderBy.getColumn()) != null) {
                        newOrderBys.add(orderBy.copy());
                    } else {
                        if (newOrderBys.size() == this.getGroupBys().size()) {
                            // 说明出现order by包含了整个group by
                            this.setGroupBys(newOrderBys);// 将group by重置一下顺序
                        }

                        // group by中不包含order by字段
                        return this.getOrderBys();
                    }
                }

                for (IOrderBy groupBy : this.getGroupBys()) {
                    if (findOrderByByColumn(newOrderBys, groupBy.getColumn()) == null) {
                        // 添加下order by中没有的字段
                        newOrderBys.add(groupBy.copy());
                    }
                }

                this.setGroupBys(newOrderBys);// 将group by重置一下顺序
                return newOrderBys;
            } else {
                return this.getOrderBys();
            }
        } else {
            // 没有orderby,复制group by
            if (this.getGroupBys() != null && !this.getGroupBys().isEmpty()) {
                return OptimizerUtils.copyOrderBys(this.getGroupBys());
            }

            return null;
        }

    }

    /**
     * 尝试查找一个同名的排序字段
     */
    protected IOrderBy findOrderByByColumn(List<IOrderBy> orderbys, ISelectable column) {
        for (IOrderBy order : orderbys) {
            if (order.getColumn().equals(column)) {
                return order;
            }
        }

        return null;
    }

    /**
     * 复制数据到目标对象，注意此方法只会复制expression对象的引用
     */
    protected void copySelfTo(QueryTreeNode to) {
        to.setAlias(this.alias);
        to.setSubAlias(this.subAlias);
        to.columnsSelected = this.getColumnsSelected();
        to.columnsRefered = this.getColumnsRefered();
        to.groups = this.getGroupBys();
        to.orderBys = this.getOrderBys();
        to.whereFilter = (IFilter) (this.whereFilter == null ? null : this.whereFilter.copy());
        to.havingFilter = (IFilter) (this.havingFilter == null ? null : havingFilter.copy());
        to.keyFilter = (IFilter) (this.keyFilter == null ? null : keyFilter.copy());
        to.resultFilter = (IFilter) (this.resultFilter == null ? null : this.resultFilter.copy());
        to.allWhereFilter = (IFilter) (this.allWhereFilter == null ? null : this.allWhereFilter.copy());
        to.otherJoinOnFilter = (IFilter) (this.otherJoinOnFilter == null ? null : this.otherJoinOnFilter.copy());
        to.setLimitFrom(this.limitFrom);
        to.setLimitTo(this.limitTo);
        to.setNeedBuild(this.needBuild);
        to.setSql(this.getSql());
        to.setSubQuery(subQuery);
        to.setExistAggregate(isExistAggregate);
    }

    /**
     * 复制数据到目标对象，注意此方法会复制expression对象
     */
    protected void deepCopySelfTo(QueryTreeNode to) {
        to.setAlias(this.alias);
        to.setSubAlias(this.subAlias);
        to.columnsSelected = OptimizerUtils.copySelectables(this.getColumnsSelected());
        to.columnsRefered = OptimizerUtils.copySelectables(this.getColumnsRefered());
        to.groups = OptimizerUtils.copyOrderBys(new ArrayList<IOrderBy>(this.getGroupBys()));
        to.orderBys = OptimizerUtils.copyOrderBys(new ArrayList<IOrderBy>(this.getOrderBys()));
        to.whereFilter = (IFilter) (this.whereFilter == null ? null : this.whereFilter.copy());
        to.havingFilter = (IFilter) (this.havingFilter == null ? null : havingFilter.copy());
        to.keyFilter = (IFilter) (this.keyFilter == null ? null : keyFilter.copy());
        to.resultFilter = (IFilter) (this.resultFilter == null ? null : this.resultFilter.copy());
        to.allWhereFilter = (IFilter) (this.allWhereFilter == null ? null : this.allWhereFilter.copy());
        to.otherJoinOnFilter = (IFilter) (otherJoinOnFilter == null ? null : otherJoinOnFilter.copy());
        to.setLimitFrom(this.getLimitFrom());
        to.setLimitTo(this.getLimitTo());
        to.setSql(this.getSql());
        to.setSubQuery(this.isSubQuery());
        to.setNeedBuild(this.isNeedBuild());
        to.setExistAggregate(isExistAggregate);
    }

}
