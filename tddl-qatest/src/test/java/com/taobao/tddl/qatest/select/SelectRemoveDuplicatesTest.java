package com.taobao.tddl.qatest.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseAndorTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.ExecuteTableName;
import com.taobao.tddl.qatest.util.Validator;

/**
 * or去重的测试
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-10-19 上午10:54:19
 */
@RunWith(Parameterized.class)
public class SelectRemoveDuplicatesTest extends BaseAndorTestCase {

    Validator validator = new Validator();

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectRemoveDuplicatesTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void prepareData() throws Exception {
        validator.con = validator.getConnection();
        validator.andorCon = us.getConnection();
        prepareData.normaltblPrepare(0, MAX_DATA_SIZE);
    }

    @After
    public void clearDate() throws Exception {
        prepareData.psConRcRsClose(rc, rs);
    }

    @Test
    public void conditionWithLessAndGreatTest() throws Exception {
        long start = 5;
        int end = 300;

        String sql = "select * from " + normaltblTableName + " where pk >? or id> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        String[] columnParam = { "PK", "NAME", "ID", "GMT_CREATE", "GMT_DATETIME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, param);
    }

}
