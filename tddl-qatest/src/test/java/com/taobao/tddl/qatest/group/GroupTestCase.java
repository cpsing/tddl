package com.taobao.tddl.qatest.group;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.qatest.BaseAtomGroupTestCase;
import com.taobao.tddl.qatest.util.LoadPropsUtil;

public class GroupTestCase extends BaseAtomGroupTestCase {

    protected static JdbcTemplate     tddlJT;
    protected static TGroupDataSource tds;

    @BeforeClass
    public static void setUp() throws Exception {
        setGroupMockInfo(GROUP_NORMAL_COMPLEX_PATH, GROUPKEY_COMPLEX);
    }

    @AfterClass
    public static void tearDown() {
        tds = null;
        tddlJT = null;
    }

    @Before
    public void init() throws Exception {
        // 清数据防止干扰
        String sql = "delete from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
    }

    @After
    public void destroy() {
        // 清数据防止干扰
        String sql = "delete from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        clearData(tddlJT, sql, new Object[] { RANDOM_ID });
    }

    protected static void setGroupMockInfo(String groupPath, String key) throws Exception {
        // 获取group信息
        String groupStr = LoadPropsUtil.loadProps2OneLine(groupPath, key);
        if (groupStr == null || StringUtils.isBlank(groupStr)) {
            throw new Exception("指定path = " + groupPath + ",key = " + key + "的group信息为null或者为空字符。");
        }

        // 获取atom信息
        dataMap = new HashMap<String, String>();
        String[] atomArr = groupStr.split(",");
        for (String atom : atomArr) {
            atom = atom.trim();
            atom = atom.substring(0, atom.indexOf(":"));
            initAtomConfig(ATOM_PATH + File.separator + atom, APPNAME, atom);
        }

        // 获取groupkey
        dataMap.put(TGroupDataSource.getFullDbGroupKey(key), groupStr);

        // 建立MockServer
        MockServer.setConfigInfos(dataMap);

        // 获取JdbcTemplate
        tddlJT = getJT(key);
    }

    protected static JdbcTemplate getJT(String dbGroupKey) {
        tds = new TGroupDataSource();
        tds.setAppName(APPNAME);
        tds.setDbGroupKey(dbGroupKey);
        tds.init();
        return new JdbcTemplate(tds);
    }

    protected void checkWeight(int total, int times, double percent) {
        Assert.assertTrue(1.0 * times / total >= percent - 0.1 && 1.0 * times / total <= percent + 0.1);
    }
}
