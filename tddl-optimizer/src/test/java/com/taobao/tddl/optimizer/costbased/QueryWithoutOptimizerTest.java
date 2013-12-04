package com.taobao.tddl.optimizer.costbased;

import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

/**
 * @author Dreamond
 */
public class QueryWithoutOptimizerTest extends BaseOptimizerTest {

    @Test
    public void test() throws Exception {
        QueryTreeNode qtn = (QueryTreeNode) new TableNode("TABLE1").executeOn("ANDOR_GROUP_0");
        qtn.build();
        System.out.println(qtn.toDataNodeExecutor());
    }
}
