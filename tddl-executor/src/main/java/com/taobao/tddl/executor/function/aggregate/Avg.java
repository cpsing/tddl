package com.taobao.tddl.executor.function.aggregate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * Avg函数处理比较特殊，会将AVG转化为SUM + COUNT，拿到所有库的数据后再计算AVG
 * 
 * @since 5.1.0
 */
public class Avg extends AggregateFunction {

    private Long        count  = 0L;
    private Number      total  = null;

    Map<String, Object> result = new HashMap<String, Object>(2);

    public void serverMap(Object[] args) throws FunctionException {
        count++;
        if (args[0] == null) {
            return;
        }
        Object o = args[0];
        Object avgRes = null;
        if (o != null) {
            if (o instanceof BigDecimal) {
                if (total == null) {
                    total = new BigDecimal(0);
                }
                total = ((BigDecimal) total).add((BigDecimal) o);
                avgRes = ((BigDecimal) total).divide(new BigDecimal(count));
            }

            if (o instanceof Integer || o instanceof Long) {
                if (o instanceof Integer) o = new BigDecimal((Integer) o);
                if (total == null) {
                    total = new BigDecimal(0);
                }
                total = ((BigDecimal) total).add(new BigDecimal((Long) o));
                avgRes = ((BigDecimal) total).divide(new BigDecimal(count), 4, RoundingMode.HALF_DOWN);
            } else if (o instanceof Float || o instanceof Double) {
                if (o instanceof Float) {
                    o = new Double((Float) o);
                }
                if (total == null) {
                    total = new BigDecimal(0);
                }

                total = ((BigDecimal) total).add(new BigDecimal((Double) o));
                avgRes = ((BigDecimal) total).divide(new BigDecimal(count));
            }
        }
        this.result.put(function.getColumnName(), avgRes);
    }

    public void serverReduce(Object[] args) throws FunctionException {
        if (args[0] == null || args[1] == null) {
            return;
        }
        String totalString = args[0].toString();
        String countString = args[1].toString();

        Number total = null;
        Long count = Long.parseLong(countString);
        this.count += count;

        try {
            total = Long.parseLong(totalString);
            if (this.total == null) {
                this.total = 0L;
            }

            this.total = ((Long) (this.total)) + (Long) total;
            result.put(function.getColumnName(), new BigDecimal(((Long) this.total) / (this.count + 0.0)));
        } catch (NumberFormatException ex) {
            try {
                total = Double.parseDouble(totalString);
                if (this.total == null) this.total = 0.0;

                this.total = ((Double) (this.total)) + (Double) total;
                result.put(function.getColumnName(), new BigDecimal(((Double) this.total) / (this.count + 0.0)));
            } catch (NumberFormatException ex2) {
                throw new FunctionException("不支持的Total类型：" + totalString);
            }
        }

    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void clear() {
        this.total = null;
        this.count = 0L;
        this.result.clear();
    }

    public DATA_TYPE getReturnType() {
        return DATA_TYPE.DOUBLE_VAL;
    }

    public DATA_TYPE getMapReturnType() {
        return DATA_TYPE.STRING_VAL;
    }

    public String getDbFunction() {
        // return "COUNT("+args[0]+"),"+"SUM("+args[0]+")";
        return bulidAvgSql(function);
    }

    private String bulidAvgSql(IFunction func) {
        String colName = func.getColumnName();
        StringBuilder sb = new StringBuilder();
        sb.append(colName.replace("AVG", "SUM"));
        sb.append(",").append(colName.replace("AVG", "COUNT"));
        return sb.toString();
    }

}
