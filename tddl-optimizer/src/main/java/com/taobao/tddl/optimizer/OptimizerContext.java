package com.taobao.tddl.optimizer;

import com.taobao.tddl.optimizer.config.Matrix;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;

/**
 * 优化器上下文，主要解决一些共享上下文对象，因为不考虑spring进行IOC控制，所以一些对象/工具之间的依赖就很蛋疼，就搞了这么一个上下文
 * 
 * @author jianghang 2013-11-12 下午3:07:19
 */
public class OptimizerContext {

    private Matrix        matrix;
    private SchemaManager schemaManager;
    private IndexManager  indexManager;

    public OptimizerContext(Matrix matrix){

    }

}
