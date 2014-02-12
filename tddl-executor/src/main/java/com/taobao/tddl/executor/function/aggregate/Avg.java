package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * Avg函数处理比较特殊，会将AVG转化为SUM + COUNT，拿到所有库的数据后再计算AVG
 * 
 * @since 5.0.0
 */
public class Avg extends AggregateFunction {

    private Long   count = 0L;
    private Object total = null;

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        count++;
        Object o = args[0];

        DataType type = getSumType();
        if (o != null) {
            if (total == null) {
                total = type.convertFrom(o);
            } else {
                total = type.getCalculator().add(total, o);
            }
        }
    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        if (args[0] == null || args[1] == null) {
            return;
        }

        count += DataType.LongType.convertFrom(args[1]);
        Object o = args[0];
        DataType type = getSumType();
        if (total == null) {
            total = type.convertFrom(o);
        } else {
            total = type.getCalculator().add(total, o);
        }
    }

    @Override
    public String getDbFunction() {
        return bulidAvgSql(function);
    }

    private String bulidAvgSql(IFunction func) {
        String colName = func.getColumnName();
        StringBuilder sb = new StringBuilder();
        if (func.getAlias() != null) {// 如果有别名，需要和FuckAvgOptimizer中保持一致
            sb.append(func.getAlias() + "1").append(",").append(func.getAlias() + "2");
        } else {
            sb.append(colName.replace("AVG", "SUM"));
            sb.append(",").append(colName.replace("AVG", "COUNT"));
        }
        return sb.toString();
    }

    @Override
    public Object getResult() {
        DataType type = this.getReturnType();
        if (total == null) {
            return type.getCalculator().divide(0L, count);
        } else {
            return type.getCalculator().divide(total, count);
        }
    }

    @Override
    public void clear() {
        this.total = null;
        this.count = 0L;
    }

    @Override
    public DataType getReturnType() {
        return getMapReturnType();
    }

    @Override
    public DataType getMapReturnType() {
        Object[] args = function.getArgs().toArray();
        DataType type = null;
        if (args[0] instanceof ISelectable) {
            type = ((ISelectable) args[0]).getDataType();
        }

        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(args[0]);
        }

        if (type == DataType.BigIntegerType) {
            // 如果是大整数，返回bigDecimal
            return DataType.BigDecimalType;
        } else {
            // 尽可能都返回为BigDecimalType，double类型容易出现精度问题，会和mysql出现误差
            // [zhuoxue.yll, 2516885.8000]
            // [zhuoxue.yll, 2516885.799999999813735485076904296875]
            // return DataType.DoubleType;
            return DataType.BigDecimalType;
        }
    }

    public DataType getSumType() {
        Object[] args = function.getArgs().toArray();
        DataType type = null;
        if (args[0] instanceof ISelectable) {
            type = ((ISelectable) args[0]).getDataType();
        }

        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(args[0]);
        }
        if (type == DataType.IntegerType || type == DataType.ShortType) {
            return DataType.LongType;
        } else {
            return type;
        }
    }
}
