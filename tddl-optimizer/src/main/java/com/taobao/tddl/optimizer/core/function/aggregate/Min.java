package com.taobao.tddl.optimizer.core.function.aggregate;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.AggregateFunction;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @author jianghang 2013-11-8 下午4:01:06
 * @since 5.1.0
 */
public class Min extends AggregateFunction {

    Object min = null;

    public Min(){
    }

    public void serverMap(Object[] args, IFunction f) throws FunctionException {
        doMin(args);

    }

    public void serverReduce(Object[] args, IFunction f) throws FunctionException {
        doMin(args);
    }

    private void doMin(Object[] args) {
        Object o = args[0];
        if (o != null) {
            if (min == null) {
                min = o;
            }
            if (((Comparable) o).compareTo(min) < 0) {
                min = o;
            }

        }
    }

    public int getArgSize() {
        return 1;
    }

    public Map<String, Object> getResult(IFunction f) {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(f.getColumnName(), min);
        return resMap;
    }

    public void clear() {
        min = null;
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return this.getMapReturnType(f);
    }

    public DATA_TYPE getMapReturnType(IFunction f) {
        Object[] args = f.getArgs().toArray();

        if (args[0] instanceof IColumn) {
            return ((IColumn) args[0]).getDataType();
        }

        if (args[0] instanceof IFunction) {
            return ((IFunction) args[0]).getDataType();
        }

        throw new NotSupportException();
    }

}
