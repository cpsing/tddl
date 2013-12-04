package com.taobao.tddl.executor.function.aggregate;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class Sum extends AggregateFunction {

    private Number total;

    public Sum(){
    }

    public void serverMap(Object[] args) throws FunctionException {
        doSum(args);
    }

    public void serverReduce(Object[] args) throws FunctionException {
        doSum(args);
    }

    private void doSum(Object[] args) {
        Object o = args[0];
        if (o != null) {
            if (o instanceof BigDecimal) {
                if (total == null) {
                    total = new BigDecimal(0);
                }

                total = ((BigDecimal) total).add((BigDecimal) o);
            }
            if (o instanceof Integer || o instanceof Long) {
                if (o instanceof Integer) {
                    o = new Long((Integer) o);
                }

                if (total == null) {
                    total = new Long(0);
                }
                total = (Long) total + ((Long) o);
            } else if (o instanceof Double) {
                if (total == null) {
                    total = new Double(0);
                }

                total = (Double) total + ((Double) o);
            } else if (o instanceof Float) {
                if (total == null) {
                    total = new Float(0);
                }

                total = (Float) total + ((Float) o);
            }

        }
    }

    public int getArgSize() {
        return 1;
    }

    public Map<String, Object> getResult() {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(function.getColumnName(), total);
        return resMap;
    }

    public void clear() {
        total = null;
    }

    public DATA_TYPE getReturnType() {
        return this.getMapReturnType();
    }

    public DATA_TYPE getMapReturnType() {
        Object[] args = function.getArgs().toArray();
        DATA_TYPE type = null;
        if (args[0] instanceof IColumn) {
            type = ((IColumn) args[0]).getDataType();
        }

        if (args[0] instanceof IFunction) {
            type = ((IFunction) args[0]).getDataType();
        }

        if (type == null) {
            return null;
        }

        if (type.equals(DATA_TYPE.LONG_VAL) || type.equals(DATA_TYPE.INT_VAL) || type.equals(DATA_TYPE.SHORT_VAL)) {
            return DATA_TYPE.LONG_VAL;
        } else if (type.equals(DATA_TYPE.FLOAT_VAL)) {
            return DATA_TYPE.FLOAT_VAL;
        } else if (type.equals(DATA_TYPE.DOUBLE_VAL)) {
            return DATA_TYPE.DOUBLE_VAL;
        }

        return null;
    }

}
