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
        String sql = "SELECT S.ID AS ID1 , T.ID AS ID2 FROM STUDENT S JOIN STUDENT T ON S.ID=T.ID AND S.ID= ? AND T.SCHOOL= ?";
        Map<String, Object> extraCmd = Maps.newHashMap();
        // extraCmd.put(ExtraCmd.JoinMergeJoin, "true");
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Integer[] { 1, 3 }), extraCmd, false);
        System.out.println(qc1);
    }

}
