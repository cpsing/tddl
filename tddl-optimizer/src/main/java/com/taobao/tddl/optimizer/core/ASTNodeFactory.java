package com.taobao.tddl.optimizer.core;

import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.core.expression.bean.BooleanFilter;
import com.taobao.tddl.optimizer.core.expression.bean.Column;
import com.taobao.tddl.optimizer.core.expression.bean.Function;
import com.taobao.tddl.optimizer.core.expression.bean.LogicalFilter;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * Ast node构建工厂，简单的工具类没必要上接口了
 */
public class ASTNodeFactory {

    private static volatile ASTNodeFactory instance = null;

    public static ASTNodeFactory getInstance() {
        if (instance == null) {
            synchronized (ASTNodeFactory.class) {
                if (instance == null) { // double-check
                    try {
                        // 预留扩展，比如生成PB协议的node对象，未来比较长远
                        instance = ExtensionLoader.load(ASTNodeFactory.class);
                    } catch (Throwable e) {
                        instance = new ASTNodeFactory();
                    }
                }
            }
        }

        return instance;
    }

    public IQuery createQuery() {
        return null;
    }

    public IReplace createReplace() {
        return null;
    }

    public IInsert createInsert() {
        return null;
    }

    public IDelete createDelete() {
        return null;
    }

    public IUpdate createUpdate() {
        return null;
    }

    public IColumn createColumn() {
        return new Column();
    }

    public IOrderBy createOrderBy() {
        return null;
    }

    public IJoin createJoin() {
        return null;
    }

    public IMerge createMerge() {
        return null;
    }

    public IBindVal createBindValue(int bind) {
        return new BindVal(bind);
    }

    public IBooleanFilter createBooleanFilter() {
        return new BooleanFilter();
    }

    public ILogicalFilter createLogicalFilter() {
        return new LogicalFilter();
    }

    public IFunction createFunction() {
        return new Function();
    }

    public Comparable createNullValue() {
        return NullValue.getNullValue();
    }
}
