package com.taobao.tddl.optimizer.core.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * DML操作树
 * 
 * @since 5.0.0
 */
public abstract class DMLNode<RT extends DMLNode> extends ASTNode<RT> {

    protected static final Logger            logger            = LoggerFactory.getLogger(DMLNode.class);
    protected List<ISelectable>              columns;
    protected List<Object>                   values;
    // 直接依赖为tableNode，如果涉及多库操作，会是一个Merge下面挂多个DML
    protected TableNode                      table             = null;
    protected Map<Integer, ParameterContext> parameterSettings = null;
    protected boolean                        needBuild         = true;

    public DMLNode(TableNode table){
        this.table = table;
    }

    public DMLNode setParameterSettings(Map<Integer, ParameterContext> parameterSettings) {
        this.parameterSettings = parameterSettings;
        return this;
    }

    public TableNode getNode() {
        return this.table;
    }

    public DMLNode setNode(TableNode table) {
        this.table = table;
        return this;
    }

    public DMLNode setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getColumns() {
        return this.columns;
    }

    public DMLNode setValues(List<Object> values) {
        this.values = values;
        return this;

    }

    public List<Object> getValues() {
        return this.values;
    }

    public TableMeta getTableMeta() {
        return getNode().getTableMeta();
    }

    @Override
    public boolean isNeedBuild() {
        return needBuild;
    }

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

    @Override
    public void build() {
        if (this.table != null) {
            table.build();
        }

        if ((this.getColumns() == null || this.getColumns().isEmpty())
            && (this.getValues() == null || this.getValues().isEmpty())) {
            return;
        }

        if (columns == null || columns.isEmpty()) { // 如果字段为空，默认为所有的字段数据的,比如insert所有字段
            columns = OptimizerUtils.columnMetaListToIColumnList(this.getTableMeta().getAllColumns(),
                this.getTableMeta().getTableName());
        }

        if (columns.size() != values.size()) {
            if (!columns.isEmpty()) {
                throw new IllegalArgumentException("The size of the columns and values is not matched."
                                                   + " columns' size is " + columns.size() + ". values' size is "
                                                   + values.size());
            }

        }

        for (ISelectable s : this.getColumns()) {
            ISelectable res = null;
            for (Object obj : table.getColumnsReferedForParent()) {
                ISelectable querySelected = (ISelectable) obj;
                if (s.isSameName(querySelected)) { // 尝试查找对应的字段信息
                    res = querySelected;
                    break;
                }
            }

            if (res == null) {
                throw new IllegalArgumentException("column: " + s.getColumnName() + " is not existed in either "
                                                   + table.getName() + " or select clause");
            }
        }

        convertTypeToSatifyColumnMeta(this.getColumns(), this.getValues());
    }

    /**
     * 尝试根据字段类型进行value转化
     */
    protected List<Object> convertTypeToSatifyColumnMeta(List<ISelectable> cs, List<Object> vs) {
        for (int i = 0; i < cs.size(); i++) {
            Comparable c = cs.get(i);
            Object v = vs.get(i);
            DataType type = null;
            if (v == null || v instanceof IBindVal || v instanceof NullValue) {
                continue;
            }

            if (c instanceof ISelectable) {
                type = ((ISelectable) c).getDataType();
            }

            vs.set(i, OptimizerUtils.convertType(v, type));
        }
        return vs;
    }

    @Override
    public RT executeOn(String dataNode) {
        super.executeOn(dataNode);
        return (RT) this;
    }

    @Override
    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        QueryTreeNode qct = getNode();

        if (qct != null) {
            qct.assignment(parameterSettings);
        }

        if (values != null) {
            List<Object> comps = new ArrayList<Object>(values.size());
            for (Object comp : values) {
                if (comp instanceof IBindVal) {
                    comps.add(((IBindVal) comp).assignment(parameterSettings));
                } else if (comp instanceof ISelectable) {
                    comps.add(((ISelectable) comp).assignment(parameterSettings));
                } else {
                    comps.add(comp);
                }
            }

            this.setValues(comps);
        }

    }

    protected void copySelfTo(DMLNode to) {
        to.columns = this.columns;
        to.values = this.values;
        to.table = this.table;
    }

    protected void deepCopySelfTo(DMLNode to) {
        to.columns = OptimizerUtils.copySelectables(this.columns);
        if (this.values != null) {
            to.values = new ArrayList(this.values.size());

            for (Object value : this.values) {
                if (value instanceof ISelectable) {
                    to.values.add(((ISelectable) value).copy());
                } else to.values.add(value);
            }
        }

        to.table = this.table.deepCopy();
    }

    @Override
    public String toString(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        OptimizerToString.appendln(sb, tabTittle + this.getClass().getSimpleName());

        OptimizerToString.appendField(sb, "columns", this.getColumns(), tabContent);
        OptimizerToString.appendField(sb, "values", this.getValues(), tabContent);

        if (this.getNode() != null) {
            OptimizerToString.appendln(sb, tabContent + "query:");
            sb.append(this.getNode().toString(inden + 2));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return this.toString(0);
    }
}
