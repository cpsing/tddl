package com.taobao.tddl.optimizer.rule;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;
import com.taobao.tddl.rule.TableRule;
import com.taobao.tddl.rule.TddlRule;
import com.taobao.tddl.rule.VirtualTableRoot;
import com.taobao.tddl.rule.exceptions.RouteCompareDiffException;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeAND;
import com.taobao.tddl.rule.model.sqljep.ComparativeBaseList;
import com.taobao.tddl.rule.model.sqljep.ComparativeMapChoicer;
import com.taobao.tddl.rule.model.sqljep.ComparativeOR;

/**
 * 优化器中使用Tddl Rule的一些工具方法，需要依赖{@linkplain TddlRule}自己先做好初始化
 * 
 * @since 5.1.0
 */
public class OptimizerRule extends AbstractLifecycle {

    private final static int DEFAULT_OPERATION_COMP = -1000;
    private final TddlRule   tddlRule;

    public OptimizerRule(TddlRule tddlRule){
        this.tddlRule = tddlRule;
    }

    @Override
    protected void doInit() throws TddlException {
        if (!tddlRule.isInited()) {
            tddlRule.init();
        }
    }

    @Override
    protected void doDestory() throws TddlException {
        if (tddlRule.isInited()) {
            tddlRule.destory();
        }
    }

    public List<TargetDB> shard(String logicTable, ComparativeMapChoicer choicer, boolean isWrite) {
        MatcherResult result;
        try {
            result = tddlRule.routeMverAndCompare(!isWrite, logicTable, choicer, Lists.newArrayList());
        } catch (RouteCompareDiffException e) {
            throw new TddlRuntimeException(e);
        }

        List<TargetDB> targetDbs = result.getCalculationResult();
        if (targetDbs == null || targetDbs.isEmpty()) {
            throw new IllegalArgumentException("can't find target db. table is " + logicTable + ".");
        }

        return targetDbs;
    }

    /**
     * 根据逻辑表和条件，计算一下目标库
     */
    public List<TargetDB> shard(String logicTable, final IFilter ifilter, boolean isWrite) {
        MatcherResult result;
        try {
            result = tddlRule.routeMverAndCompare(!isWrite, logicTable, new ComparativeMapChoicer() {

                @Override
                public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
                    Map<String, Comparative> map = new HashMap<String, Comparative>();
                    for (String str : partnationSet) {
                        map.put(str, getColumnComparative(arguments, str));
                    }

                    return map;
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, String colName) {
                    return getComparative(ifilter, colName);
                }
            }, Lists.newArrayList());
        } catch (RouteCompareDiffException e) {
            throw new TddlRuntimeException(e);
        }

        List<TargetDB> targetDbs = result.getCalculationResult();
        if (targetDbs == null || targetDbs.isEmpty()) {
            throw new IllegalArgumentException("can't find target db. table is " + logicTable + ". filter is "
                                               + ifilter);
        }

        return targetDbs;
    }

    /**
     * 允许定义期望的expectedGroups，如果broadcast的逻辑表的物理拓扑结构包含了该节点，那说明可以做本地节点join，下推sql
     */
    public List<TargetDB> shardBroadCast(String logicTable, final IFilter ifilter, boolean isWrite,
                                         List<String> expectedGroups) {
        if (expectedGroups == null) {
            return this.shard(logicTable, ifilter, isWrite);
        }

        if (!isBroadCast(logicTable)) {
            throw new TddlRuntimeException(logicTable + "不是broadCast的表");
        }

        List<TargetDB> targets = this.shard(logicTable, (IFilter) null, isWrite);
        List<TargetDB> targetsMatched = new ArrayList<TargetDB>();
        for (TargetDB target : targets) {
            if (expectedGroups.contains(target.getDbIndex())) {
                targetsMatched.add(target);
            }
        }

        return targetsMatched;
    }

    /**
     * 根据逻辑表返回一个随机的物理目标库TargetDB
     * 
     * @param logicTable
     * @return
     */
    public TargetDB shardAny(String logicTable) {
        TableRule tableRule = getTableRule(logicTable);
        if (tableRule == null) {
            // 设置为同名，同名不做转化
            TargetDB target = new TargetDB();
            target.setDbIndex(getDefaultDbIndex(logicTable, tddlRule.getCurrentRule()));
            target.addOneTable(logicTable);
            return target;
        } else {
            for (String group : tableRule.getActualTopology().keySet()) {
                Set<String> tableNames = tableRule.getActualTopology().get(group);
                if (tableNames == null || tableNames.isEmpty()) {
                    continue;
                }

                TargetDB target = new TargetDB();
                target.setDbIndex(group);
                target.addOneTable(tableNames.iterator().next());
                return target;
            }
        }
        throw new IllegalArgumentException("can't find any target db. table is " + logicTable + ". ");
    }

    public String getDefaultGroup() {
        VirtualTableRoot root = tddlRule.getCurrentRule();
        return root.getDefaultDbIndex();
    }

    public String getJoinGroup(String logicTable) {
        TableRule table = getTableRule(logicTable);
        return table != null ? table.getJoinGroup() : null;// 没找到表规则，默认为单库
    }

    public boolean isBroadCast(String logicTable) {
        TableRule table = getTableRule(logicTable);
        return table != null ? table.isBroadcast() : false;// 没找到表规则，默认为单库，所以不是广播表
    }

    public List<String> getSharedColumns(String logicTable) {
        TableRule table = getTableRule(logicTable);
        return table != null ? table.getShardColumns() : new ArrayList<String>();// 没找到表规则，默认为单库
    }

    private TableRule getTableRule(String logicTable) {
        VirtualTableRoot root = tddlRule.getCurrentRule();
        TableRule table = root.getVirtualTable(logicTable);
        return table;
    }

    private String getDefaultDbIndex(String vtab, VirtualTableRoot vtrCurrent) {
        Map<String, String> dbIndexMap = vtrCurrent.getDbIndexMap();
        if (dbIndexMap != null && dbIndexMap.get(vtab) != null) {
            return dbIndexMap.get(vtab);
        }
        return vtrCurrent.getDefaultDbIndex();
    }

    /**
     * 将一个{@linkplain IFilter}表达式转化为Tddl Rule所需要的{@linkplain Comparative}对象
     * 
     * @param ifilter
     * @param colName
     * @return
     */
    public static Comparative getComparative(IFilter ifilter, String colName) {
        // 前序遍历，找到所有符合要求的条件
        if (ifilter == null) {
            return null;
        }

        if ("NOT".equalsIgnoreCase(ifilter.getFunctionName())) {
            return null;
        }

        if (ifilter instanceof ILogicalFilter) {
            if (ifilter.isNot()) {
                return null;
            }

            ComparativeBaseList comp = null;
            ILogicalFilter logicalFilter = (ILogicalFilter) ifilter;
            switch (ifilter.getOperation()) {
                case AND:
                    comp = new ComparativeAND();
                    break;
                case OR:
                    comp = new ComparativeOR();
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            for (Object sub : logicalFilter.getSubFilter()) {
                if (!(sub instanceof IFilter)) {
                    return null;
                }

                IFilter subFilter = (IFilter) sub;
                Comparative subComp = getComparative(subFilter, colName);// 递归
                if (subComp != null) {
                    comp.addComparative(subComp);
                }

            }

            if (comp == null || comp.getList() == null || comp.getList().isEmpty()) {
                return null;
            } else if (comp.getList().size() == 1) {
                return comp.getList().get(0);// 可能只有自己一个and
            }
            return comp;
        } else if (ifilter instanceof IBooleanFilter) {
            Comparative comp = null;
            IBooleanFilter booleanFilter = (IBooleanFilter) ifilter;

            // 判断非空
            if (booleanFilter.getColumn() == null
                || (booleanFilter.getValue() == null && booleanFilter.getValues() == null)) {
                return null;
            }

            // 判断是否为 A > B , A > B + 1
            if (booleanFilter.getColumn() instanceof ISelectable && booleanFilter.getValue() instanceof ISelectable) {
                return null;
            }

            // 判断是否为 A > ALL(subquery)
            if (booleanFilter.getColumn() instanceof QueryTreeNode || booleanFilter.getValue() instanceof QueryTreeNode) {
                return null;
            }

            // 必须要有一个是字段
            if (!(booleanFilter.getColumn() instanceof IColumn || booleanFilter.getValue() instanceof IColumn)) {
                return null;
            }

            if (booleanFilter.isNot()) {
                return null;
            }

            if (booleanFilter.getOperation() == OPERATION.IN) {// in不能出现isReverse
                ComparativeBaseList orComp = new ComparativeOR();
                for (Object value : booleanFilter.getValues()) {
                    IBooleanFilter ef = ASTNodeFactory.getInstance().createBooleanFilter();
                    ef.setOperation(OPERATION.EQ);
                    ef.setColumn(booleanFilter.getColumn());
                    ef.setValue(value);

                    Comparative subComp = getComparative(ef, colName);
                    if (subComp != null) {
                        orComp.addComparative(subComp);
                    }
                }

                if (orComp.getList().isEmpty()) {// 所有都被过滤
                    return null;
                }

                return orComp;
            } else {
                int operationComp = DEFAULT_OPERATION_COMP;
                switch (booleanFilter.getOperation()) {
                    case GT:
                        operationComp = Comparative.GreaterThan;
                        break;
                    case EQ:
                        operationComp = Comparative.Equivalent;
                        break;
                    case GT_EQ:
                        operationComp = Comparative.GreaterThanOrEqual;
                        break;
                    case LT:
                        operationComp = Comparative.LessThan;
                        break;
                    case LT_EQ:
                        operationComp = Comparative.LessThanOrEqual;
                        break;
                    default:
                        return null;
                }

                IColumn column = null;
                Object value = null;
                if (booleanFilter.getColumn() instanceof IColumn) {
                    column = OptimizerUtils.getColumn(booleanFilter.getColumn());
                    value = getComparableWhenTypeIsNowReturnDate(booleanFilter.getValue());
                } else {// 出现 1 = id 的写法
                    column = OptimizerUtils.getColumn(booleanFilter.getValue());
                    value = getComparableWhenTypeIsNowReturnDate(booleanFilter.getColumn());
                    operationComp = Comparative.exchangeComparison(operationComp); // 反转一下
                }

                if (colName.equalsIgnoreCase(column.getColumnName()) && operationComp != DEFAULT_OPERATION_COMP) {
                    if (!(value instanceof Comparable)) {
                        throw new TddlRuntimeException("type: " + value.getClass().getSimpleName()
                                                       + " is not comparable, cannot be used in partition column");
                    }
                    comp = new Comparative(operationComp, (Comparable) value);
                }

                return comp;
            }
        } else {
            // 为null,全表扫描
            return null;
        }
    }

    private static Object getComparableWhenTypeIsNowReturnDate(Object val) {
        if (val instanceof IFunction) {
            IFunction func = (IFunction) val;
            if ("NOW".equalsIgnoreCase(func.getFunctionName())) {
                return new Date();
            }
        }

        return val;
    }
}
