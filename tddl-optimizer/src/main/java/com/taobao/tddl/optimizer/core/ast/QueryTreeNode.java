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
 * @since 5.1.0
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
    protected IFilter           allWhereFilter     = null;

    /**
     * select查询中的列
     */
    protected List<ISelectable> columnsSelected    = new ArrayList<ISelectable>();

    /**
     * 不在select中，但是出现在其它的地方，如order by中的列，where条件中的列，一些隐藏的列
     */
    protected List<ISelectable> implicitSelectable = new ArrayList<ISelectable>();

    /**
     * 依赖的所有列，可以理解为columnsSelected + implicitSelectable的总合
     */
    protected List<ISelectable> columnsRefered     = new ArrayList<ISelectable>();

    /**
     * 显式的由查询接口指定的orderBy，注意需要保证顺序
     */
    protected List<IOrderBy>    orderBys           = new LinkedList<IOrderBy>();

    /**
     * 显式的由查询接口指定的group by，注意需要保证顺序
     */
    protected List<IOrderBy>    groups             = new LinkedList<IOrderBy>();

    /**
     * having条件
     */
    protected IFilter           havingFilter;

    /**
     * join的子节点
     */
    protected List<ASTNode>     children           = new ArrayList<ASTNode>(2);

    /**
     * 从哪里开始
     */
    protected Comparable        limitFrom          = null;

    /**
     * 到哪里结束
     */
    protected Comparable        limitTo            = null;

    /**
     * filter in where
     */
    protected IFilter           whereFilter        = null;

    /**
     * 非column=column的join列
     */
    protected IFilter           otherJoinOnFilter;

    /**
     * 当前qn的别名，用于进行join等操作的时候辨别到底这一行是从哪个subNode来的。
     */
    protected String            alias;

    protected boolean           consistent         = true;

    protected LOCK_MODEL        lockModel          = LOCK_MODEL.SHARED_LOCK;

    /**
     * 当前节点是否为子查询
     */
    protected boolean           subQuery           = false;
    protected boolean           needBuild          = true;
    protected QUERY_CONCURRENCY queryConcurrency;

    /**
     * 获取完整的order by列信息(包括隐藏，比如group by会转化为order by)
     */
    public abstract List<IOrderBy> getImplicitOrderBys();

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
        this.implicitSelectable = OptimizerUtils.assignment(implicitSelectable, parameterSettings);
        this.otherJoinOnFilter = OptimizerUtils.assignment(this.otherJoinOnFilter, parameterSettings);
        if (this.limitFrom instanceof IBindVal) {
            limitFrom = ((IBindVal) limitFrom).assignment(parameterSettings);
        }

        if (this.limitTo instanceof IBindVal) {
            limitTo = ((IBindVal) limitTo).assignment(parameterSettings);
        }
    }

    // =================== build / copy =========================

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

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
        return (ASTNode) this.getChildren().get(0);
    }

    public void addChild(ASTNode childNode) {
        this.children.add(childNode);
    }

    public List<ISelectable> getImplicitSelectable() {
        return implicitSelectable;
    }

    public void setImplicitSelectable(List<ISelectable> implicitSelectable) {
        this.implicitSelectable = implicitSelectable;
    }

    /**
     * 添加一个不存在的字段
     */
    public void addImplicitSelectable(ISelectable selectable) {
        if (!this.implicitSelectable.contains(selectable)) {
            this.implicitSelectable.add(selectable);
        }
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
        if (!this.columnsSelected.contains(selected)) {
            columnsSelected.add(selected);
        }
    }

    /**
     * 添加一个不存在的字段
     */
    public void addColumnsRefered(ISelectable selected) {
        if (!this.columnsRefered.contains(selected)) {
            columnsRefered.add(selected);
        }
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

    public QueryTreeNode join(String rightTable, String rightFilter, String leftKey, String rightKey) {
        return this.join(new TableNode(rightTable).query(rightFilter)).addJoinKeys(leftKey, rightKey);
    }

    public QueryTreeNode join(String rightTable, String rightFilter) {
        return this.join(new TableNode(rightTable).query(rightFilter));
    }

    public QueryTreeNode join(String rightTable, String leftKey, String rightKey) {
        return this.join(new TableNode(rightTable)).addJoinKeys(leftKey, rightKey);
    }

    public QueryTreeNode join(QueryTreeNode rightNode, String leftKey, String rightKey) {
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

    public boolean isSubQuery() {
        return subQuery;
    }

    public void setSubQuery(boolean subQuery) {
        setNeedBuild(true);
        this.subQuery = subQuery;
    }

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

    public boolean hasColumn(ISelectable c) {
        return this.getBuilder().hasColumn(c);
    }

    // ==================== helper method =================

    protected List<IOrderBy> getOrderByCombineWithGroupBy() {
        // 如果用户没指定order by，则显示index的order by
        if (this.getOrderBys() != null && !this.getOrderBys().isEmpty()) {
            if (this.getGroupBys() != null && !this.getGroupBys().isEmpty()) {
                if (this.getGroupBys().size() <= this.getOrderBys().size()) {
                    return this.getOrderBys();
                }

                List<IOrderBy> orderByCombineWithGroupBy = new ArrayList();

                /**
                 * 如果order by中的列与group by中的都一样，且order by的列少于group by的列
                 * 则应将最后的group by的列补到order by中 <br/>
                 * 如：group by c1 c2 order by c1 等价于group by c1 c2 order by c1 c2
                 */
                for (int i = 0; i < this.getOrderBys().size(); i++) {
                    if (this.getGroupBys().get(i).getColumn().equals(this.getOrderBys().get(i).getColumn())) {
                        orderByCombineWithGroupBy.add(this.getOrderBys().get(i));
                    } else {
                        return this.getOrderBys();
                    }
                }

                for (int i = this.getOrderBys().size(); i < this.getGroupBys().size(); i++) {
                    orderByCombineWithGroupBy.add(this.getGroupBys().get(i).deepCopy());
                }

                return orderByCombineWithGroupBy;
            } else {
                return this.getOrderBys();
            }
        }

        if (this.getGroupBys() != null && !this.getGroupBys().isEmpty()) {
            return OptimizerUtils.copyOrderBys(this.getGroupBys());
        }

        return null;
    }

    /**
     * 复制数据到目标对象，注意此方法只会复制expression对象的引用
     */
    protected void copySelfTo(QueryTreeNode to) {
        to.setAlias(this.alias);
        to.columnsSelected = this.getColumnsSelected();
        to.implicitSelectable = this.getImplicitSelectable();
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
    }

    /**
     * 复制数据到目标对象，注意此方法会复制expression对象
     */
    protected void deepCopySelfTo(QueryTreeNode to) {
        to.setAlias(this.alias);
        to.columnsSelected = OptimizerUtils.deepCopySelectableList(this.getColumnsSelected());
        to.implicitSelectable = OptimizerUtils.deepCopySelectableList(this.getImplicitSelectable());
        to.columnsRefered = OptimizerUtils.deepCopySelectableList(this.getColumnsRefered());
        to.groups = OptimizerUtils.deepCopyOrderByList(new ArrayList<IOrderBy>(this.getGroupBys()));
        to.orderBys = OptimizerUtils.deepCopyOrderByList(new ArrayList<IOrderBy>(this.getOrderBys()));
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
    }

}
