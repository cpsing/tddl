package com.taobao.tddl.optimizer.costbased;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;

public class RuleTest extends BaseOptimizerTest {

    @Test
    public void testQueryNoCondition() throws SqlParserException, QueryException {
        String sql = "select s.id as id1 , t.id as id2 from student s join student t on s.id=t.id and s.id= ? and t.school= ?";
        Map<String, Comparable> extraCmd = Maps.newHashMap();
        // extraCmd.put(ExtraCmd.OptimizerExtraCmd.JoinMergeJoin, "true");
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Integer[] { 1, 3 }), extraCmd, false);
        System.out.println(qc1);
    }

}
