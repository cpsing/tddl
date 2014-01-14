package com.taobao.tddl.optimizer.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlExprVisitor;
import com.taobao.tddl.optimizer.utils.range.AndRangeProcessor;
import com.taobao.tddl.optimizer.utils.range.OrRangeProcessor;

/**
 * 用来做一些布尔表达式的转换，比如我们会将(A and B) OR C => (A and C) or (A and B)，析取范式便于做计算<br/>
 * 注意，目前处理中不是一个严格的析取范式处理，比如 A and B and C不会进行转化
 * 
 * <pre>
 * DNF析取范式: 
 *  a. http://zh.wikipedia.org/zh-cn/析取范式
 *  b. http://baike.baidu.com/view/143339.htm
 *  
 * 简单析取式: 仅由有限个文字构成的析取式，比如：p,q,p∨q
 * 析取范式：由有限个简单合取式构成的析取式，比如 (p∧q)vr
 * </pre>
 * 
 * @author Dreamond
 * @author jianghang 2013-11-13 下午1:18:53
 */
public class FilterUtils {

    // ----------------------- DNF filter处理-------------------------

    /**
     * 将一个Bool树转换成析取形式 A（B+C）转换为AB+AC
     * 
     * @param node
     * @return
     */
    public static IFilter toDNFAndFlat(IFilter node) {
        if (node == null) {
            return null;
        }

        node = toDNF(node);
        node = flatDNFFilter(node);
        return node;
    }

    /**
     * 将一个Bool树转换成析取形式 A（B+C）转换为AB+AC，不做拉平处理
     */
    public static IFilter toDNF(IFilter node) {
        if (node == null) {
            return null;
        }

        while (!isDNF(node)) {
            if (node.getOperation().equals(OPERATION.OR)) {
                node = passOrNode((ILogicalFilter) node);
            } else if (node.getOperation().equals(OPERATION.AND)) {
                node = expandAndNode((ILogicalFilter) node);
            }
        }

        return node;
    }

    private static IFilter passOrNode(ILogicalFilter node) {
        for (int i = 0; i < node.getSubFilter().size(); i++) {
            node.getSubFilter().set(i, toDNF(node.getSubFilter().get(i)));
        }
        return node;
    }

    private static IFilter expandAndNode(ILogicalFilter node) {
        if (node.getSubFilter().size() > 2) {
            throw new IllegalArgumentException("此处不支持And包含超过两个子节点\n" + node);
        }

        if (node.getSubFilter().size() == 1) {
            return node;
        }

        if ((!isLogicalNode(node.getSubFilter().get(0))) && (!isLogicalNode(node.getSubFilter().get(1)))) {
            return node;
        }
        node.setLeft(toDNF(node.getLeft()));
        node.setRight(toDNF(node.getRight()));
        // (A+B)C = AC+BC
        boolean isRightOr = node.getRight().getOperation().equals(OPERATION.OR);
        boolean isLeftOr = node.getLeft().getOperation().equals(OPERATION.OR);
        if (isLeftOr || isRightOr) {
            IFilter orNode = node.getLeft();
            IFilter otherNode = node.getRight();
            if (isRightOr) {
                orNode = node.getRight();
                otherNode = node.getLeft();
            }

            ILogicalFilter leftAnd = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
            ILogicalFilter rightAnd = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
            // 构造 AC
            leftAnd.setLeft(((ILogicalFilter) orNode).getLeft());
            leftAnd.setRight(otherNode);
            // 构造 BC
            rightAnd.setLeft(((ILogicalFilter) orNode).getRight());
            rightAnd.setRight(otherNode);
            // 构造AC + BC
            ILogicalFilter or = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.OR);
            or.addSubFilter(leftAnd).addSubFilter(rightAnd);
            return passOrNode(or);
        }
        return node;
    }

    /**
     * 拉平一个filter树，将多层的嵌套拉平为一层<br/>
     * 比如： A and B and (C and D) => A and B and C and D
     */
    private static IFilter flatDNFFilter(IFilter node) {
        if (!isDNF(node)) {
            throw new IllegalArgumentException("filter is not dnf!\n" + node);
        }

        List<List<IFilter>> dnfNodes = toDNFNodesArray(node);
        if (dnfNodes.size() == 1 && dnfNodes.get(0).size() == 1) {
            return dnfNodes.get(0).get(0);
        }

        ILogicalFilter or = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.OR);
        for (List<IFilter> dnfNode : dnfNodes) {
            if (dnfNode.size() != 1) {
                ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
                for (IFilter boolNode : dnfNode) {
                    and.addSubFilter(boolNode);
                }

                if (dnfNodes.size() == 1) { // 如果只有一个合取，直接返回合取结果
                    return and;
                }

                or.addSubFilter(and);
            } else {
                or.addSubFilter(dnfNode.get(0));
            }
        }

        return or;
    }

    /**
     * 将一个IFilter全部展开为一个多维数组，会做拉平处理，需要预先调用toDNF/toDNFAndFlat进行预处理转化为DNF范式
     * 
     * <pre>
     * 比如：(A and B) or (A and C)
     * 返回结果为： 
     *   List- 
     *      List 
     *         -(A , B)
     *      List
     *         -(A , C)
     * </pre>
     */
    public static List<List<IFilter>> toDNFNodesArray(IFilter node) {
        if (node == null || !isDNF(node)) {
            return Lists.newLinkedList(); // 返回空的数组节点
        }

        List<List<IFilter>> res = new LinkedList();
        if (node.getOperation().equals(OPERATION.OR)) {
            for (int i = 0; i < ((ILogicalFilter) node).getSubFilter().size(); i++) {
                res.addAll(toDNFNodesArray(((ILogicalFilter) node).getSubFilter().get(i)));
            }
        } else if (node.getOperation().equals(OPERATION.AND)) {
            res.add(toDNFNode(node));
        } else {
            List<IFilter> DNFNode = new ArrayList<IFilter>(1);
            DNFNode.add(node);
            res.add(DNFNode);
        }

        if (res == null || res.isEmpty() || res.get(0) == null || res.get(0).isEmpty() || res.get(0).get(0) == null) {
            return new LinkedList<List<IFilter>>();
        } else {
            return res;
        }

    }

    /**
     * 将一个IFilter全部展开为一个平级的数组，不考虑逻辑and/or的组织关系<br/>
     * 需要预先调用toDNF/toDNFAndFlat进行预处理转化为DNF范式
     */
    public static List<IFilter> toDNFNode(IFilter node) {
        List<IFilter> DNFNode = Lists.newLinkedList();
        if (node == null) {
            return DNFNode;
        }

        if (!isLogicalNode(node)) {
            DNFNode.add(node);
            return DNFNode;
        }

        for (int i = 0; i < ((ILogicalFilter) node).getSubFilter().size(); i++) {
            if (!isLogicalNode(((ILogicalFilter) node).getSubFilter().get(i))) {
                DNFNode.add(((ILogicalFilter) node).getSubFilter().get(i));
            } else {
                // 递归处理
                DNFNode.addAll(toDNFNode(((ILogicalFilter) node).getSubFilter().get(i)));
            }
        }

        return DNFNode;
    }

    /**
     * 根据column进行filter归类
     */
    public static Map<Object, List<IFilter>> toColumnFiltersMap(List<IFilter> DNFNode) {
        Map<Object, List<IFilter>> columns = new HashMap(DNFNode.size());
        for (IFilter boolNode : DNFNode) {
            if (!columns.containsKey(((IBooleanFilter) boolNode).getColumn())) {
                columns.put(((IBooleanFilter) boolNode).getColumn(), new LinkedList());
            }

            columns.get(((IBooleanFilter) boolNode).getColumn()).add(boolNode);
        }
        return columns;
    }

    /**
     * 非严格DNF检查，允许出现 Filter(A and B)
     */
    public static boolean isDNF(IFilter node) {
        if (!isLogicalNode(node)) {
            return true;
        }

        if (node.getOperation().equals(OPERATION.AND)) {
            boolean isAllBooleanFilter = true;
            for (IFilter sub : ((ILogicalFilter) node).getSubFilter()) {
                if (isLogicalNode(sub)) {
                    isAllBooleanFilter = false;
                    break;
                }
            }
            if (isAllBooleanFilter) {
                return true;
            }

            for (IFilter sub : ((ILogicalFilter) node).getSubFilter()) {
                if (sub.getOperation().equals(OPERATION.OR)) { // 子表达式中存在析取
                    return false;
                }
            }
        }

        for (IFilter sub : ((ILogicalFilter) node).getSubFilter()) {
            if (!isDNF(sub)) {
                return false;
            }
        }

        return true;
    }

    /**
     * 是否为一简单合取式
     */
    public static boolean isCNFNode(IFilter node) {
        if (node == null) {
            return false;
        }

        if (node.getOperation().equals(OPERATION.AND)) {
            for (IFilter f : ((ILogicalFilter) node).getSubFilter())
                if (!isCNFNode(f)) {
                    return false;
                }

        } else if (node.getOperation().equals(OPERATION.OR)) {
            return false;
        }

        return true;
    }

    /**
     * 判断是否为and/or的组合节点
     */
    private static boolean isLogicalNode(IFilter node) {
        if (node instanceof ILogicalFilter) {
            return true;
        }

        return false;
    }

    // -------------------- 智能merge 处理 -----------------

    /**
     * 将filter中的and/or条件中进行Range合并处理 <br/>
     * 
     * <pre>
     * 比如: 
     *  a. A =1 And A =2 ，永远false条件，返回EmptyResultFilterException异常
     *  b. (1 < A < 5) or (2 < A < 6)，合并为 (1 < A < 6)
     *  c. A <= 1 or A = 1，永远true条件
     * </pre>
     */
    public static IFilter merge(IFilter filter) throws EmptyResultFilterException {
        if (filter == null || filter instanceof IBooleanFilter) {
            return filter;
        }
        // 先转为DNF结构
        filter = toDNFAndFlat(filter);
        List<List<IFilter>> DNFNodes = toDNFNodesArray(filter);
        if (!needToMerge(DNFNodes)) {
            return filter;
        }

        DNFNodes = mergeOrDNFNodes(mergeAndDNFNodesArray(DNFNodes));
        if (DNFNodes == null || DNFNodes.isEmpty() || DNFNodes.get(0) == null || DNFNodes.get(0).isEmpty()
            || DNFNodes.get(0).get(0) == null) {
            // 返回常量true
            IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
            f.setOperation(OPERATION.CONSTANT);
            f.setColumn("1");
            f.setColumnName(ObjectUtils.toString("1"));
            return f;
        } else {
            return DNFToOrLogicTree(DNFNodes);
        }
    }

    /**
     * 如果filter中包含函数，或者是绑定变量，则不进行merge
     */
    private static boolean needToMerge(List<List<IFilter>> dNFNodes) {
        for (List<IFilter> DNFNode : dNFNodes) {
            for (IFilter filter : DNFNode) {
                if (((IBooleanFilter) filter).getValue() instanceof IBindVal
                    || ((IBooleanFilter) filter).getValue() instanceof IFunction) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 合并析取式中的And重复条件
     */
    private static List<List<IFilter>> mergeAndDNFNodesArray(List<List<IFilter>> DNFNodesBeforeMerge)
                                                                                                     throws EmptyResultFilterException {
        List<List<IFilter>> nodesAfterMerge = new LinkedList();
        for (List<IFilter> DNFNode : DNFNodesBeforeMerge) {
            // 每个合取中按照column进行归类
            Map<Comparable, List<IFilter>> columnRestrictions = new HashMap();
            for (IFilter boolNode : DNFNode) {
                Comparable c = (Comparable) ((IBooleanFilter) boolNode).getColumn();
                if (!columnRestrictions.containsKey(c)) {
                    columnRestrictions.put(c, new LinkedList());
                }
                columnRestrictions.get(c).add(boolNode);
            }

            // 针对单个字段的条件进行合并，比如 A > 1 and A < 5 and A > 3合并为 3 < A < 5
            List<IFilter> columnsFilter = new LinkedList();
            for (Comparable c : columnRestrictions.keySet()) {
                AndRangeProcessor ri = new AndRangeProcessor(c);
                for (IFilter node : columnRestrictions.get(c)) {
                    if (!ri.process(node)) {
                        throw new EmptyResultFilterException("空结果");
                    }
                }
                List<IFilter> boolNodesOfCurrentColumn = ri.toFilterList();
                columnsFilter.addAll(boolNodesOfCurrentColumn);
            }

            nodesAfterMerge.add(columnsFilter);
        }

        return nodesAfterMerge;
    }

    /**
     * 合并析取式中的Or重复条件
     */
    private static List<List<IFilter>> mergeOrDNFNodes(List<List<IFilter>> DNFNodes) throws EmptyResultFilterException {
        Map<Object, List<IFilter>> columnRestrictions = new HashMap<Object, List<IFilter>>();
        List<List<IFilter>> toRemove = new LinkedList<List<IFilter>>();
        for (List<IFilter> DNFNode : DNFNodes) {
            if (DNFNode.size() == 1) { // 只处理单个or条件的表达式，比如 A = 1 or A < 2
                Object c = (((IBooleanFilter) DNFNode.get(0)).getColumn());
                if (!columnRestrictions.containsKey(c)) {
                    columnRestrictions.put(c, new LinkedList());
                }
                columnRestrictions.get(c).add(DNFNode.get(0));
                toRemove.add(DNFNode);
            }
        }

        DNFNodes.removeAll(toRemove); // 先干掉，后面会计算后会重新添加
        for (Object c : columnRestrictions.keySet()) {
            OrRangeProcessor ri = new OrRangeProcessor(c);
            for (IFilter boolNode : columnRestrictions.get(c)) {
                ri.process(boolNode);
            }

            if (ri.isFullSet()) {
                return new LinkedList<List<IFilter>>();
            } else {
                DNFNodes.addAll(ri.toFilterList());
            }
        }
        return DNFNodes;
    }

    /**
     * 将析取范式的数组重新构造为一个LogicFilter，使用and/or条件
     * 
     * @param DNFNodes
     * @return
     */
    public static IFilter DNFToOrLogicTree(List<List<IFilter>> DNFNodes) {
        if (DNFNodes.isEmpty()) {
            return null;
        }

        IFilter treeNode = DNFToAndLogicTree(DNFNodes.get(0));
        for (int i = 1; i < DNFNodes.size(); i++) {
            treeNode = or(treeNode, DNFToAndLogicTree(DNFNodes.get(i)));
        }
        return treeNode;
    }

    /**
     * 将一系列的boolean filter ，拼装成一个andFilter { boolFilter , boolFilter...}
     * 的filter..
     * 
     * @param DNFNode
     * @return
     */
    public static IFilter DNFToAndLogicTree(List<IFilter> DNFNode) {
        if (DNFNode == null || DNFNode.isEmpty()) {
            return null;
        }
        IFilter treeNode = DNFNode.get(0);
        for (int i = 1; i < DNFNode.size(); i++) {
            treeNode = and(treeNode, DNFNode.get(i));
        }
        return treeNode;
    }

    // -------------------- filter helper method-----------------

    /**
     * 创建and条件
     */
    public static IFilter and(IFilter root, IFilter o) {
        if (o == null) {
            return root;
        }

        if (root == null) {
            root = o;
        } else {
            if (root.getOperation().equals(OPERATION.AND)) {
                ((ILogicalFilter) root).addSubFilter(o);
            } else {
                ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
                and.addSubFilter(root);
                and.addSubFilter(o);
                root = and;
            }
        }
        return root;
    }

    /**
     * 创建or条件
     */
    public static IFilter or(IFilter root, IFilter o) {
        if (o == null) {
            return root;
        }

        if (root == null) {
            root = o;
        } else {
            if (root.getOperation().equals(OPERATION.OR)) {
                ((ILogicalFilter) root).addSubFilter(o);
            } else {
                ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.OR);
                and.addSubFilter(root);
                and.addSubFilter(o);
                root = and;
            }
        }
        return root;
    }

    /**
     * 创建equal filter
     */
    public static IBooleanFilter equal(Comparable columnName, Comparable value) {
        IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
        f.setOperation(OPERATION.EQ);
        f.setColumn(columnName);
        f.setValue(value);
        return f;
    }

    /**
     * 判断是否为常量的filter
     */
    public static boolean isConstFilter(IBooleanFilter f) {
        if (f.getColumn() instanceof IColumn || f.getColumn() instanceof IFunction) {
            return false;
        }

        if (f.getValue() instanceof IColumn || f.getValue() instanceof IFunction) {
            return false;
        }

        return true;
    }

    /**
     * 基于字符串表达式构建IFilter
     */
    public static IFilter createFilter(String where) {
        if (StringUtils.isEmpty(where)) {
            return null;
        }

        MySqlExprVisitor visitor = MySqlExprVisitor.parser(where);
        Comparable value = visitor.getColumnOrValue();
        if (value instanceof IFilter) {
            return (IFilter) value;
        } else if (value instanceof ISelectable) {
            throw new IllegalArgumentException("不合法的filter表达式:" + where);
        } else {
            return visitor.buildConstanctFilter(value);
        }
    }

    /**
     * 判断是否为常量的表达式对象
     */
    public static boolean isConstValue(Object v) {
        if (v instanceof IColumn || v instanceof IFunction) {
            return false;
        }

        return true;
    }

}
