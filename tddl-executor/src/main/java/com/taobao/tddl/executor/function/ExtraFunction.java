package com.taobao.tddl.executor.function;

import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

public abstract class ExtraFunction implements IExtraFunction {

    protected IFunction function;
    protected Object    result;

    @Override
    public void setFunction(IFunction function) {
        this.function = function;
    }

    /**
     * 如果可以用db的函数，那就直接使用
     * 
     * @param function
     */
    protected abstract String getDbFunction();

    protected List getReduceArgs(IFunction func) {
        String resArgs = getDbFunction();
        Object[] obs = resArgs.split(",");
        return Arrays.asList(obs);
    }

    protected List getMapArgs(IFunction func) {
        return func.getArgs();
    }

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args
     * @throws FunctionException
     */
    public abstract void serverMap(Object[] args) throws FunctionException;

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args
     * @throws FunctionException
     */
    public abstract void serverReduce(Object[] args) throws FunctionException;

    /**
     * 外部执行器传递ResultSet中的row记录，进行function的map计算
     * 
     * @param kvPair
     * @throws Exception
     */
    public void serverMap(IRowSet kvPair) throws TddlRuntimeException {
        // 当前function需要的args 有些可能是函数，也有些是其他的一些数据
        List<Object> argsArr = getMapArgs(function);
        // 函数的input参数
        Object[] inputArg = new Object[argsArr.size()];
        int index = 0;
        for (Object funcArg : argsArr) {
            if (funcArg instanceof IFunction) {
                if (((IFunction) funcArg).getExtraFunction().getFunctionType().equals(FunctionType.Aggregate)) {
                    // aggregate function
                    inputArg[index] = ((AggregateFunction) ((IFunction) funcArg).getExtraFunction()).getResult();
                    index++;
                } else {
                    // scalar function
                    ((ScalarFunction) ((IFunction) funcArg).getExtraFunction()).serverMap(kvPair);
                    inputArg[index] = ((ScalarFunction) ((IFunction) funcArg).getExtraFunction()).getResult();
                    index++;
                }
            } else if (funcArg instanceof ISelectable) {// 如果是IColumn，那么应该从输入的参数中获取对应column
                if (IColumn.STAR.equals(((ISelectable) funcArg).getColumnName())) {
                    inputArg[index] = kvPair;
                } else {
                    inputArg[index] = ExecUtils.getValueByIColumn(kvPair, ((ISelectable) funcArg));
                }
                index++;
            } else {
                inputArg[index] = funcArg;
                index++;
            }
        }

        serverMap(inputArg);
    }

    /**
     * 外部执行器传递ResultSet中的row记录，进行function的reduce计算
     * 
     * @param kvPair
     * @throws Exception
     */
    public void serverReduce(IRowSet kvPair) throws TddlRuntimeException {
        if (this.getFunctionType().equals(FunctionType.Scalar)) {
            if (this.getClass().equals(Dummy.class)) {
                Integer index = kvPair.getParentCursorMeta().getIndex(null, this.function.getColumnName());
                if (index != null) {
                    Object v = kvPair.getObject(index);
                    ((ScalarFunction) this).setResult(v);
                } else {
                    throw new TddlRuntimeException(this.function.getFunctionName() + " 没有实现，结果集中也没有");
                }
            } else {
                // 目前存在一问题，如果是row+1的字段，tddl还会再计算一次
                try {
                    this.serverMap(kvPair);
                } catch (RuntimeException e) {
                    Integer index = kvPair.getParentCursorMeta().getIndex(null, this.function.getColumnName());
                    if (index != null) {
                        Object v = kvPair.getObject(index);
                        ((ScalarFunction) this).setResult(v);
                    } else {
                        throw e;
                    }
                }
            }
        } else {
            // 函数的input参数
            List<Object> reduceArgs = this.getReduceArgs(function);
            Object[] inputArg = new Object[reduceArgs.size()];
            // 目前认为所有scalar函数可下推
            for (int i = 0; i < reduceArgs.size(); i++) {
                String name = reduceArgs.get(i).toString();
                Object val = ExecUtils.getValueByTableAndName(kvPair, this.function.getTableName(), name);
                inputArg[i] = val;
            }

            serverReduce(inputArg);
        }
    }

    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void clear() {
        result = null;
    }

}
