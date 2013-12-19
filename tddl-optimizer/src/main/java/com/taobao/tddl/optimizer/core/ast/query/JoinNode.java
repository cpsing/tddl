package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.JoinNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class JoinNode extends QueryTreeNode {

    private JoinNodeBuilder      builder;
    /**
     * join 策略
     */
    private JoinStrategy         joinStrategy          = JoinStrategy.NEST_LOOP_JOIN;

    /**
     * <pre>
     * leftOuterJoin:
     *      leftOuter=true && rightOuter=false
     * rightOuterJoin:
     *      leftOuter=false && rightOuter=true
     * innerJoin:
     *      leftOuter=false && rightOuter=false 
     * outerJoin:
     *      leftOuter=true && rightOuter=true
     * </pre>
     */
    private boolean              leftOuter             = false;
    private boolean              rightOuter            = false;

    private boolean              isCrossJoin           = false;
    private boolean              usedForIndexJoinPK    = false;
    private List<IBooleanFilter> joinFilter            = new ArrayList();

    private boolean              needOptimizeJoinOrder = true;

    public JoinNode(){
        builder = new JoinNodeBuilder(this);
    }

    public List<ISelectable> getLeftKeys() {
        List<ISelectable> leftKeys = new ArrayList<ISelectable>(this.getJoinFilter().size());
        for (IBooleanFilter f : this.getJoinFilter()) {
            leftKeys.add((ISelectable) f.getColumn());
        }
        return leftKeys;
    }

    public List<ISelectable> getRightKeys() {
        List<ISelectable> rightKeys = new ArrayList<ISelectable>(this.getJoinFilter().size());
        for (IBooleanFilter f : this.getJoinFilter()) {
            rightKeys.add((ISelectable) f.getValue());
        }
        return rightKeys;
    }

    public JoinNode addJoinKeys(ISelectable leftKey, ISelectable rightKey) {
        this.joinFilter.add(FilterUtils.equal(leftKey, rightKey));
        setNeedBuild(true);
        return this;
    }

    public JoinNode addJoinKeys(String leftKey, String rightKey) {
        return this.addJoinKeys(OptimizerUtils.createColumnFromString(leftKey),
            OptimizerUtils.createColumnFromString(rightKey));
    }

    public void addJoinFilter(IBooleanFilter filter) {
        this.joinFilter.add(filter);
        setNeedBuild(true);
    }

    public QueryTreeNode getLeftNode() {
        if (this.getChildren() == null || this.getChildren().isEmpty()) {
            return null;
        }
        return (QueryTreeNode) this.getChildren().get(0);
    }

    public QueryTreeNode getRightNode() {
        if (this.getChildren() == null || this.getChildren().size() < 2) {
            return null;
        }

        return (QueryTreeNode) this.getChildren().get(1);
    }

    public void setLeftNode(QueryTreeNode left) {
        if (this.getChildren().isEmpty()) {
            this.getChildren().add(left);
        } else {
            this.getChildren().set(0, left);
        }
        setNeedBuild(true);
    }

    public void setRightNode(QueryTreeNode right) {
        if (this.getChildren().isEmpty()) {
            this.getChildren().add(null);
        }
        if (this.getChildren().size() == 1) {
            this.getChildren().add(right);
        } else {
            this.getChildren().set(1, right);
        }

        setNeedBuild(true);
    }

    public List<ASTNode> getChildren() {
        List<ASTNode> childs = super.getChildren();
        childs.remove(null);// 删除left为null的情况
        return childs;
    }

    public List<IOrderBy> getImplicitOrderBys() {
        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        }

        List<IOrderBy> orders = new ArrayList();
        // index nested loop以左表顺序为准
        if (this.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP
            || this.getJoinStrategy() == JoinStrategy.NEST_LOOP_JOIN) {
            orders = this.getLeftNode().getImplicitOrderBys();
        } else if (this.getJoinStrategy() == JoinStrategy.SORT_MERGE_JOIN) {
            // sort merge的话，返回空值，由上层来判断
            return orders;
        }

        List<IOrderBy> implicitOrdersCandidate = orders;
        List<IOrderBy> implicitOrders = new ArrayList();
        for (int i = 0; i < implicitOrdersCandidate.size(); i++) {
            if (this.getColumnsSelected().contains(implicitOrdersCandidate.get(i).getColumn())) {
                implicitOrders.add(implicitOrdersCandidate.get(i));
            } else {
                break;
            }
        }

        return implicitOrders;
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public String getName() {
        return this.getAlias();
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.getLeftNode().build();
            this.getRightNode().build();
            this.builder.build();
        }

        setNeedBuild(false);
    }

    /**
     * 交换左右节点
     */
    public void exchangeLeftAndRight() {
        setNeedBuild(true);

        QueryTreeNode tmp = this.getLeftNode();
        this.setLeftNode(this.getRightNode());
        this.setRightNode(tmp);

        boolean tmpouter = this.leftOuter;
        this.leftOuter = this.rightOuter;
        this.rightOuter = tmpouter;

    }

    public IJoin toDataNodeExecutor() throws QueryException {
        IJoin join = ASTNodeFactory.getInstance().createJoin();
        join.setRightNode(this.getRightNode().toDataNodeExecutor());
        join.setLeftNode(this.getLeftNode().toDataNodeExecutor());
        join.setJoinStrategy(this.getJoinStrategy());
        join.setLeftOuter(this.getLeftOuter()).setRightOuter(this.getRightOuter());
        join.setJoinOnColumns((this.getLeftKeys()), (this.getRightKeys()));
        join.setOrderBys(this.getOrderBys());
        join.setLimitFrom(this.getLimitFrom()).setLimitTo(this.getLimitTo());
        join.executeOn(this.getDataNode()).setConsistent(true);
        join.setValueFilter(this.getResultFilter());
        join.having(this.getHavingFilter());
        join.setAlias(this.getAlias());
        join.setGroupBys(this.getGroupBys());
        join.setIsSubQuery(this.isSubQuery());
        join.setOtherJoinOnFilter(this.getOtherJoinOnFilter());
        if (this.isCrossJoin()) {
            join.setColumns(new ArrayList(0)); // 查询所有字段
        } else {
            join.setColumns((this.getColumnsSelected()));
        }

        join.setWhereFilter(this.getAllWhereFilter());
        return join;
    }

    public QueryTreeNode convertToJoinIfNeed() {
        super.convertToJoinIfNeed(); // 首先执行一次TableNode处理，生成join

        // 如果右边是子查询，join策略为block，不做调整
        if (!(this.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP)) {
            return this;
        }

        QueryTreeNode right = this.getRightNode();

        // 如果右边是一个IQuery，就按正常的方法生成JoinNode即可
        if (right instanceof TableNode || right instanceof QueryNode || right instanceof MergeNode) {
            return this;
        }

        assert (right instanceof JoinNode);// 右边也是一个join
        // 将原本 A join (B Join C) 调整为 (A join B) join C
        // 原本B join C可能是TableNode中走的索引信息不包含字段信息，需要做回表查询
        QueryTreeNode left = (QueryTreeNode) this.getLeftNode();
        if (right instanceof JoinNode) {
            QueryTreeNode rightIndexQuery = ((JoinNode) right).getLeftNode();
            QueryTreeNode rightKeyQuery = ((JoinNode) right).getRightNode();

            JoinNode leftJoinRightIndex = left.join(rightIndexQuery);
            leftJoinRightIndex.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);

            // 复制join的右字段，修正一下表名
            List<ISelectable> rightIndexJoinOnColumns = OptimizerUtils.copySelectables(this.getRightKeys(),
                rightIndexQuery.getName());

            // 添加left join index的条件
            for (int i = 0; i < this.getLeftKeys().size(); i++) {
                leftJoinRightIndex.addJoinKeys(this.getLeftKeys().get(i), rightIndexJoinOnColumns.get(i));
            }

            leftJoinRightIndex.setLeftRightJoin(this.leftOuter, this.rightOuter);
            leftJoinRightIndex.executeOn(this.getDataNode());
            List<ISelectable> leftJoinRightIndexColumns = new LinkedList();
            // 复制left的查询
            List<ISelectable> leftJoinColumns = OptimizerUtils.copySelectables(left.getColumnsSelected(),
                left.getName());

            // 复制index的查询
            List<ISelectable> rightIndexColumns = OptimizerUtils.copySelectables(rightIndexQuery.getColumnsSelected(),
                rightIndexQuery.getName());

            leftJoinRightIndexColumns.addAll(leftJoinColumns);
            leftJoinRightIndexColumns.addAll(rightIndexColumns);
            // left + index的查询做为新的join查询字段
            leftJoinRightIndex.select(leftJoinRightIndexColumns);

            // (left join index) join key构建
            JoinNode leftJoinRightIndexJoinRightKey = leftJoinRightIndex.join(rightKeyQuery);
            leftJoinRightIndexJoinRightKey.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP); // 也是走index

            List<ISelectable> leftKeys = OptimizerUtils.copySelectables(((JoinNode) right).getLeftKeys(),
                rightIndexQuery.getName());
            for (int i = 0; i < leftKeys.size(); i++) {
                leftJoinRightIndexJoinRightKey.addJoinKeys(leftKeys.get(i), ((JoinNode) right).getRightKeys().get(i));
            }

            leftJoinRightIndexJoinRightKey.setLeftRightJoin(this.leftOuter, this.rightOuter);
            leftJoinRightIndexJoinRightKey.setOrderBys(this.getOrderBys());
            leftJoinRightIndexJoinRightKey.setConsistent(true);
            leftJoinRightIndexJoinRightKey.setLimitFrom(this.getLimitFrom());
            leftJoinRightIndexJoinRightKey.setLimitTo(this.getLimitTo());
            leftJoinRightIndexJoinRightKey.setAlias(this.getAlias());
            leftJoinRightIndexJoinRightKey.setSubAlias(this.getSubAlias());
            leftJoinRightIndexJoinRightKey.executeOn(this.getDataNode());

            if (this.isCrossJoin()) {
                leftJoinRightIndexJoinRightKey.select(new ArrayList(0));// 查全表所有字段，build的时候会补充
            } else {
                leftJoinRightIndexJoinRightKey.setColumnsSelected(this.getColumnsSelected());
            }

            leftJoinRightIndexJoinRightKey.setGroupBys(this.getGroupBys());
            leftJoinRightIndexJoinRightKey.setResultFilter(this.getResultFilter());
            leftJoinRightIndexJoinRightKey.setOtherJoinOnFilter(this.getOtherJoinOnFilter());
            leftJoinRightIndexJoinRightKey.setSubQuery(this.isSubQuery());
            leftJoinRightIndexJoinRightKey.setAllWhereFilter(this.getAllWhereFilter());
            leftJoinRightIndexJoinRightKey.build();
            return leftJoinRightIndexJoinRightKey;
        }

        return this;
    }

    // ===================== setter / getter =========================

    public JoinStrategy getJoinStrategy() {
        return this.joinStrategy;
    }

    public JoinNode setJoinStrategy(JoinStrategy joinStrategy) {
        this.joinStrategy = joinStrategy;
        return this;
    }

    public List<IBooleanFilter> getJoinFilter() {
        return this.joinFilter;
    }

    public void setJoinFilter(List<IBooleanFilter> joinFilter) {
        this.joinFilter = joinFilter;
    }

    public JoinNode setCrossJoin() {
        this.isCrossJoin = true;
        return this;
    }

    public boolean isCrossJoin() {
        return isCrossJoin;
    }

    public JoinNode setLeftOuterJoin() {
        this.leftOuter = true;
        this.rightOuter = false;
        return this;
    }

    public JoinNode setRightOuterJoin() {
        this.rightOuter = true;
        this.leftOuter = false;
        return this;
    }

    public JoinNode setInnerJoin() {
        this.leftOuter = false;
        this.rightOuter = false;
        return this;
    }

    /**
     * 或者称为full join
     */
    public JoinNode setOuterJoin() {
        this.leftOuter = true;
        this.rightOuter = true;
        return this;
    }

    public boolean getLeftOuter() {
        return this.leftOuter;
    }

    public boolean getRightOuter() {
        return this.rightOuter;
    }

    public boolean isLeftOuterJoin() {
        return (this.getLeftOuter()) && (!this.getRightOuter());
    }

    public boolean isRightOuterJoin() {
        return (!this.getLeftOuter()) && (this.getRightOuter());
    }

    public boolean isInnerJoin() {
        return (!this.getLeftOuter()) && (!this.getRightOuter());
    }

    public boolean isOuterJoin() {
        return (this.getLeftOuter()) && (this.getRightOuter());
    }

    public boolean isNeedOptimizeJoinOrder() {
        return this.needOptimizeJoinOrder;
    }

    public void setNeedOptimizeJoinOrder(boolean needOptimizeJoinOrder) {
        this.needOptimizeJoinOrder = needOptimizeJoinOrder;
    }

    public boolean isUedForIndexJoinPK() {
        return usedForIndexJoinPK;
    }

    public void setUsedForIndexJoinPK(boolean uedForIndexJoinPK) {
        this.usedForIndexJoinPK = uedForIndexJoinPK;
    }

    public JoinNode setLeftRightJoin(boolean leftOuter, boolean rightOuter) {
        this.leftOuter = leftOuter;
        this.rightOuter = rightOuter;
        return this;
    }

    public JoinNode copy() {
        JoinNode newJoinNode = new JoinNode();
        this.copySelfTo(newJoinNode);
        newJoinNode.setJoinFilter(new ArrayList<IBooleanFilter>(this.getJoinFilter()));
        newJoinNode.setJoinStrategy(this.getJoinStrategy());
        newJoinNode.setLeftNode((QueryTreeNode) this.getLeftNode().copy());
        newJoinNode.setRightNode((QueryTreeNode) this.getRightNode().copy());
        newJoinNode.setNeedOptimizeJoinOrder(this.isNeedOptimizeJoinOrder());
        newJoinNode.isCrossJoin = this.isCrossJoin;
        newJoinNode.leftOuter = this.leftOuter;
        newJoinNode.rightOuter = this.rightOuter;
        newJoinNode.usedForIndexJoinPK = this.usedForIndexJoinPK;
        return newJoinNode;
    }

    public JoinNode deepCopy() {
        JoinNode newJoinNode = new JoinNode();
        this.deepCopySelfTo(newJoinNode);
        newJoinNode.setJoinFilter(OptimizerUtils.copyFilter(this.getJoinFilter()));
        newJoinNode.setJoinStrategy(this.getJoinStrategy());
        newJoinNode.setLeftNode((QueryTreeNode) this.getLeftNode().deepCopy());
        newJoinNode.setRightNode((QueryTreeNode) this.getRightNode().deepCopy());
        newJoinNode.setNeedOptimizeJoinOrder(this.isNeedOptimizeJoinOrder());
        newJoinNode.isCrossJoin = this.isCrossJoin;
        newJoinNode.leftOuter = this.leftOuter;
        newJoinNode.rightOuter = this.rightOuter;
        newJoinNode.usedForIndexJoinPK = this.usedForIndexJoinPK;
        return newJoinNode;
    }

    public String toString(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "Join" + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "Join");
        }

        appendField(sb, "joinFilter:", this.getJoinFilter(), tabContent);
        appendField(sb, "otherJoinOnFilter:", this.getOtherJoinOnFilter(), tabContent);

        if (this.isInnerJoin()) {
            appendField(sb, "type", "inner join", tabContent);
        }
        if (this.isRightOuterJoin()) {
            appendField(sb, "type", "right outter join", tabContent);
        }
        if (this.isLeftOuterJoin()) {
            appendField(sb, "type", "left outter join", tabContent);
        }
        if (this.isOuterJoin()) {
            appendField(sb, "type", "outer join", tabContent);
        }

        appendField(sb, "resultFilter", printFilterString(this.getResultFilter(), inden + 2), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter(), inden + 2), tabContent);
        // appendField(sb, "allWhereFilter",
        // printFilterString(this.getAllWhereFilter()), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter(), inden + 2), tabContent);
        if (!(this.getLimitFrom() != null && this.getLimitFrom().equals(0L) && this.getLimitTo() != null && this.getLimitTo()
            .equals(0L))) {
            appendField(sb, "limitFrom", this.getLimitFrom(), tabContent);
            appendField(sb, "limitTo", this.getLimitTo(), tabContent);
        }

        if (this.isSubQuery()) {
            appendField(sb, "isSubQuery", this.isSubQuery(), tabContent);
        }
        appendField(sb, "orderBys", this.getOrderBys(), tabContent);
        appendField(sb, "queryConcurrency", this.getQueryConcurrency(), tabContent);
        appendField(sb, "columns", this.getColumnsSelected(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "strategy", this.getJoinStrategy(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        appendln(sb, tabContent + "left:");
        sb.append(this.getLeftNode().toString(inden + 2));
        appendln(sb, tabContent + "right:");
        sb.append(this.getRightNode().toString(inden + 2));
        return sb.toString();
    }
}
