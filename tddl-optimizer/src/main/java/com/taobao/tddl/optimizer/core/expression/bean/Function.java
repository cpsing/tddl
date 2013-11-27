package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.IRowSet;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.function.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.function.IExtraFunction;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.function.scalar.Dummy;

/**
 * @since 5.1.0
 */
public class Function<RT extends IFunction> implements IFunction<RT> {

    public static final Object[] emptyArgs       = new Object[0];
    // count
    protected String             functionName;
    protected List               args            = new ArrayList();
    protected String             alias;
    protected boolean            distinct        = false;
    protected String             tablename;

    // count(id)
    protected String             columnName;
    protected IExtraFunction     extraFunction;
    private boolean              isNot;
    private boolean              needDistinctArg = false;

    public String getFunctionName() {
        return functionName;
    }

    public IFunction setFunctionName(String functionName) {
        this.functionName = functionName;
        return this;
    }

    public RT setDataType(DATA_TYPE dataType) {
        throw new NotSupportException();
    }

    public DATA_TYPE getDataType() {
        throw new NotSupportException();
    }

    public String getAlias() {
        return alias;
    }

    public String getTableName() {
        return tablename;
    }

    public String getColumnName() {
        return columnName;
    }

    public RT setAlias(String alias) {
        this.alias = alias;
        return (RT) this;
    }

    public RT setTableName(String tableName) {
        this.tablename = tableName;
        return (RT) this;
    }

    public RT setColumnName(String columnName) {
        this.columnName = columnName;
        return (RT) this;
    }

    public boolean isSameName(ISelectable select) {
        String cn1 = this.getColumnName();
        if (TStringUtil.isNotEmpty(this.getAlias())) {
            cn1 = this.getAlias();
        }

        String cn2 = select.getColumnName();
        if (TStringUtil.isNotEmpty(select.getAlias())) {
            cn2 = select.getAlias();
        }

        return TStringUtil.equals(cn1, cn2);
    }

    public String getFullName() {
        return this.getColumnName();
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isNot() {
        return isNot;
    }

    public RT setDistinct(boolean distinct) {
        this.distinct = distinct;
        return (RT) this;
    }

    public RT setIsNot(boolean isNot) {
        this.isNot = isNot;
        return (RT) this;
    }

    public FunctionType getFunctionType() {
        return getExtraFunction().getFunctionType();
    }

    public List getMapArgs() {
        return args;
    }

    public List getReduceArgs() {
        String resArgs = getExtraFunction().getDbFunction(this);
        Object[] obs = resArgs.split(",");
        return Arrays.asList(obs);
    }

    public List getArgs() {
        return args;
    }

    public RT setArgs(List args) {
        this.args = args;
        return (RT) this;
    }

    public int getExtraFuncArgSize() {
        return getExtraFunction().getArgSize();
    }

    public IExtraFunction getExtraFunction() {
        if (extraFunction == null) {
            extraFunction = ExtraFunctionManager.getExtraFunction(getFunctionName());
        }

        if (extraFunction == null) {// 仍然未找到
            extraFunction = new Dummy();
        }

        return extraFunction;
    }

    public boolean isNeedDistinctArg() {
        return needDistinctArg;
    }

    public RT setNeedDistinctArg(boolean b) {
        this.needDistinctArg = b;
        return (RT) this;
    }

    public void clear() {
        this.getExtraFunction().clear();
    }

    public RT copy() {
        IFunction funcNew = ASTNodeFactory.getInstance().createFunction();
        funcNew.setFunctionName(getFunctionName())
            .setAlias(this.getAlias())
            .setTableName(this.getTableName())
            .setColumnName(this.getColumnName())
            .setDistinct(this.isDistinct())
            .setIsNot(this.isNot());

        if (getMapArgs() != null) {
            List<Object> argsNew = new ArrayList(getMapArgs().size());
            for (Object arg : getMapArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.add(((ISelectable) arg).copy());
                } else if (arg instanceof IBindVal) {
                    argsNew.add(((IBindVal) arg));
                } else {
                    argsNew.add(arg);
                }

            }
            funcNew.setArgs(argsNew);
        }
        return (RT) funcNew;
    }

    /**
     * 复制一下function属性
     */
    protected void copy(IFunction funcNew) {
        funcNew.setFunctionName(getFunctionName())
            .setAlias(this.getAlias())
            .setTableName(this.getTableName())
            .setColumnName(this.getColumnName())
            .setDistinct(this.isDistinct())
            .setIsNot(this.isNot());

        if (getMapArgs() != null) {
            List<Object> argsNew = new ArrayList(getMapArgs().size());
            for (Object arg : getMapArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.add(((ISelectable) arg).copy());
                } else if (arg instanceof IBindVal) {
                    argsNew.add(((IBindVal) arg));
                } else {
                    argsNew.add(arg);
                }

            }
            funcNew.setArgs(argsNew);
        }
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    public Object getResult() {
        return getExtraFunction().getResult(this);
    }

    public void setExtraFunction(IExtraFunction extraFunction) {
        this.extraFunction = extraFunction;
    }

    public void serverMap(IRowSet kvPair) throws Exception {
        // 当前function需要的args 有些可能是函数，也有些是其他的一些数据
        List<Object> argsArr = getArgs();
        // 函数的input参数
        Object[] inputArg = new Object[argsArr.size()];
        int index = 0;
        for (Object funcArg : argsArr) {
            if (funcArg instanceof IFunction && ((IFunction) funcArg).getFunctionType().equals(FunctionType.Aggregate)) {
                Map<String, Object> resMap = (Map<String, Object>) ((IFunction) funcArg).getResult();
                for (Object ob : resMap.values()) {
                    inputArg[index] = ob;
                    index++;
                }
            } else if (funcArg instanceof ISelectable) {// 如果是IColumn，那么应该从输入的参数中获取对应column
                if (IColumn.STAR.equals(((ISelectable) funcArg).getColumnName())) {
                    inputArg[index] = kvPair;
                } else {
                    ((ISelectable) funcArg).serverMap(kvPair);
                    inputArg[index] = ((ISelectable) funcArg).getResult();
                }
                index++;
            } else {
                inputArg[index] = funcArg;
                index++;
            }
        }

        getExtraFunction().serverMap(inputArg, this);
    }

    public void serverReduce(IRowSet kvPair) throws Exception {
        // TODO
        if (this.getFunctionType().equals(FunctionType.Scalar)) {
            if (this.getExtraFunction() instanceof Dummy) {
                // Integer index = kvPair.getParentCursorMeta().getIndex(null,
                // this.getColumnName());
                Integer index = 0;
                if (index != null) {
                    Object v = kvPair.getObject(index);
                    ((ScalarFunction) getExtraFunction()).setResult(v);
                } else {
                    throw new NotSupportException(this.getFunctionName() + " 没有实现，结果集中也没有");
                }
            } else {
                try {
                    this.serverMap(kvPair);
                } catch (Exception e) {
                    // Integer index =
                    // kvPair.getParentCursorMeta().getIndex(null,
                    // this.getColumnName());
                    Integer index = 0;
                    if (index != null) {
                        Object v = kvPair.getObject(index);
                        ((ScalarFunction) getExtraFunction()).setResult(v);
                    } else {
                        throw e;
                    }
                }

            }
        } else {
            // 函数的input参数
            List<Object> reduceArgs = this.getReduceArgs();
            Object[] inputArg = new Object[reduceArgs.size()];
            for (int i = 0; i < reduceArgs.size(); i++) {
                String name = reduceArgs.get(i).toString();
                // Object val = GeneralUtil.getValueByTableAndName(kvPair,
                // getTableName(), name);
                Object val = null;
                inputArg[i] = val;
            }

            getExtraFunction().serverReduce(inputArg, this);
        }
    }

    public RT assignment(Map<Integer, ParameterContext> parameterSettings) {
        if (getMapArgs() != null) {
            List<Object> argsNew = getMapArgs();
            int index = 0;
            for (Object arg : getMapArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.set(index, ((ISelectable) arg).assignment(parameterSettings));
                } else if (arg instanceof IBindVal) {
                    argsNew.set(index, ((IBindVal) arg).assignment(parameterSettings));
                } else {
                    argsNew.set(index, arg);
                }
                index++;
            }
            this.setArgs(argsNew);
        }

        return (RT) this;
    }

    public String toString() {
        return this.getColumnName();
    }

}
