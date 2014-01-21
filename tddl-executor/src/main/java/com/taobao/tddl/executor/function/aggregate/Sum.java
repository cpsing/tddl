package com.taobao.tddl.executor.function.aggregate;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class Sum extends AggregateFunction {

    private Object total;

    public Sum(){
    }

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        doSum(args);
    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        doSum(args);
    }

    private void doSum(Object[] args) {
        Object o = args[0];

        DataType type = this.getMapReturnType();

        if (total == null) {

            o = type.convertFromObject(o);

            total = o;
        } else {
            total = type.getCalculator().add(total, o);
        }

        // if (o != null) {
        // if (o instanceof BigDecimal) {
        // if (total == null) {
        // total = new BigDecimal(0);
        // }
        //
        // total = ((BigDecimal) total).add((BigDecimal) o);
        // }
        // if (o instanceof Integer || o instanceof Long) {
        // if (o instanceof Integer) {
        // o = new Long((Integer) o);
        // }
        //
        // if (total == null) {
        // total = new Long(0);
        // }
        // total = (Long) total + ((Long) o);
        // } else if (o instanceof Double) {
        // if (total == null) {
        // total = new Double(0);
        // }
        //
        // total = (Double) total + ((Double) o);
        // } else if (o instanceof Float) {
        // if (total == null) {
        // total = new Float(0);
        // }
        //
        // total = (Float) total + ((Float) o);
        // }
        //
        // }
    }

    public int getArgSize() {
        return 1;
    }

    @Override
    public Map<String, Object> getResult() {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(function.getColumnName(), total);
        return resMap;
    }

    @Override
    public void clear() {
        total = null;
    }

    @Override
    public DataType getReturnType() {
        return this.getMapReturnType();
    }

    @Override
    public DataType getMapReturnType() {
        Object[] args = function.getArgs().toArray();
        DataType type = null;
        if (args[0] instanceof ISelectable) {
            type = ((ISelectable) args[0]).getDataType();
        } else {
            type = DataTypeUtil.getTypeOfObject(args[0]);
        }

        if (type == null) {
            return null;
        }

        if (type == DataType.LongType || type == DataType.IntType || type == DataType.ShortType) {
            return DataType.LongType;
        } else if (type == DataType.FloatType) {
            return DataType.FloatType;
        } else if (type == DataType.DoubleType) {
            return DataType.DoubleType;
        }

        return null;
    }

}
