package com.taobao.tddl.optimizer.core.ast.build;

import java.util.List;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.1.0
 */
public abstract class QueryTreeNodeBuilder {

    protected QueryTreeNode node;

    public QueryTreeNodeBuilder(){
    }

    public QueryTreeNode getNode() {
        return node;
    }

    public void setNode(QueryTreeNode node) {
        this.node = node;
    }

    public abstract void build();

    protected void addSelectableToSelected(ISelectable c) {
        if (!node.getColumnsSelected().contains(c)) {
            node.getColumnsSelected().add(c);
        }
    }

    protected void addSelectableToImplicitSelectable(ISelectable c) {
        if (!node.getImplicitSelectable().contains(c)) {
            node.getImplicitSelectable().add(c);
        }
    }

    protected void addSelectableToRefered(ISelectable c) {
        if (!node.getColumnsRefered().contains(c)) {
            node.getColumnsRefered().add(c);
        }
    }

    protected void buildWhere() {
        // sql语法中，where条件中的列不允许使用别名，所以无需从select中找列
        this.buildFilter(node.getKeyFilter(), false);
        this.buildFilter(node.getWhereFilter(), false);
        this.buildFilter(node.getResultFilter(), false);
        this.buildFilter(node.getOtherJoinOnFilter(), false);
    }

    protected void buildFilter(IFilter filter, boolean findInSelectList) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                this.buildFilter(sub, findInSelectList);
            }
        } else {
            buildBooleanFilter((IBooleanFilter) filter, findInSelectList);
        }
    }

    protected void buildBooleanFilter(IBooleanFilter filter, boolean findInSelectList) {
        if (filter == null) {
            return;
        }
        if (filter.getColumn() instanceof ISelectable) {
            filter.setColumn(this.buildSelectable((ISelectable) filter.getColumn(), findInSelectList));
        }

        if (filter.getValue() instanceof ISelectable) {
            filter.setValue(this.buildSelectable((ISelectable) filter.getValue(), findInSelectList));
        }

    }

    public ISelectable buildSelectable(ISelectable c) {
        return this.buildSelectable(c, false);
    }

    /**
     * 用于标记当前节点是否需要根据meta信息填充信息
     * 
     * <pre>
     * SQL. 
     *  a. select id + 2 as id , id from test where id = 2 having id = 4;
     *  b. select id + 2 as id , id from test where id = 2 order by count(id)
     * 
     * 解释：
     * 1.  COLUMN/WHERE/JOIN中列，是取自FROM的表字段
     * 2.  HAVING/ORDER BY/GROUP BY中的列，是取自SELECT中返回的字段，获取对应别名数据
     * </pre>
     * 
     * @param c
     * @param findInSelectList 如果在from的meta中找不到，是否继续在select中寻找
     * @return
     */
    public ISelectable buildSelectable(ISelectable c, boolean findInSelectList) {
        if (c == null) {
            return null;
        }

        if (c.getTableName() != null) {
            // 对于TableNode如果别名存在别名
            if (node instanceof TableNode && (!(node instanceof KVIndexNode))) {
                if (!(c.getTableName().equals(node.getAlias()) || c.getTableName()
                    .equals(((TableNode) node).getTableName()))) {
                    throw new IllegalArgumentException("column: " + c.getFullName() + " is not existed in either "
                                                       + this.getNode().getName() + " or select clause");
                }

                c.setTableName(((TableNode) node).getTableName());
            }
        }

        ISelectable column = null;
        ISelectable columnFromMeta = null;
        // 临时列中也不存在，则新建一个临时列
        columnFromMeta = this.getSelectableFromChild(c);
        if (columnFromMeta != null) {
            column = columnFromMeta;
            column.setAlias(c.getAlias());
            column.setDistinct(c.isDistinct());
        }

        if (findInSelectList) {
            ISelectable columnFromSelected = getColumnFromSelecteList(c);
            if (columnFromSelected != null) {
                column = columnFromSelected;
            }
        }

        if (column == null) {
            throw new IllegalArgumentException("column: " + c.getFullName() + " is not existed in either "
                                               + this.getNode().getName() + " or select clause");
        }

        if ((column instanceof IColumn) && !IColumn.STAR.equals(column.getColumnName())) {
            addSelectableToRefered(column);
        }

        if (column instanceof IFunction) {
            buildFunction((IFunction) column);
        }

        return column;
    }

    /**
     * 从select列表中查找
     */
    private ISelectable getColumnFromSelecteList(ISelectable c) {
        ISelectable column = null;
        for (ISelectable selected : this.getNode().getColumnsSelected()) {
            boolean isThis = false;

            if (c.getTableName() != null && (!(node instanceof KVIndexNode))) {
                if (!c.getTableName().equals(selected.getTableName())) {
                    continue;
                }
            }

            isThis = c.isSameName(selected);
            if (isThis) {
                column = selected;
                return column;
            }
        }

        return column;
    }

    public abstract ISelectable getSelectableFromChild(ISelectable c);

    public void buildOrderBy() {
        for (IOrderBy order : node.getOrderBys()) {
            if (order.getColumn() instanceof ISelectable) {
                order.setColumn(this.buildSelectable((ISelectable) order.getColumn(), true));
            }
        }
    }

    public void buildGroupBy() {
        for (IOrderBy order : node.getGroupBys()) {
            if (order.getColumn() instanceof ISelectable) {
                order.setColumn(this.buildSelectable((ISelectable) order.getColumn(), true));
            }
        }
    }

    public void buildHaving() {
        // having是允许使用select中的列的，如 havaing count(id)>1
        this.buildFilter(this.getNode().getHavingFilter(), true);
    }

    public void buildFunction() {
        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (selected instanceof IFunction) {
                this.buildFunction((IFunction) selected);
            }
        }
    }

    public void buildFunction(IFunction f) {
        if (f.getArgs().size() == 0) {
            return;
        }

        List<Object> args = f.getArgs();
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof ISelectable) {
                args.set(i, this.buildSelectable((ISelectable) args.get(i)));
            }
        }
    }

    public boolean hasColumn(ISelectable c) {
        if (this.getColumnFromOtherNode(c, this.getNode()) != null) {
            return true;
        }

        if (this.getSelectableFromChild(c) != null) {
            return true;
        }

        return false;
    }

    public ISelectable getColumnFromOtherNode(ISelectable c, QueryTreeNode other) {
        if (c == null) {
            return c;
        }

        if (c instanceof IBooleanFilter && ((IBooleanFilter) c).getOperation().equals(OPERATION.CONSTANT)) {
            return c;
        }

        ISelectable res = null;
        for (ISelectable selected : other.getColumnsSelected()) {
            boolean isThis = false;
            if (c.getTableName() != null) {
                if (!(c.getTableName().equals(other.getAlias()) || c.getTableName().equals(selected.getTableName()))) {
                    continue;
                }
            }

            if (IColumn.STAR.equals(c.getColumnName())) {
                return c;
            }

            // 若列别名存在，只比较别名
            isThis = c.isSameName(selected);

            if (isThis) {
                res = selected;
                break;
            }
        }

        if (res == null) {
            return res;
        }

        // TODO 列的tableName的信息更新，放在另外一个函数调用更合适
        if (c instanceof IColumn) {
            c.setDataType(res.getDataType());
            // 如果存在表别名，在这里将只是用表别名
            if (other.getAlias() != null) {
                c.setTableName(other.getAlias());
            } else {
                c.setTableName(res.getTableName());
            }
        }

        return c;
    }

}
