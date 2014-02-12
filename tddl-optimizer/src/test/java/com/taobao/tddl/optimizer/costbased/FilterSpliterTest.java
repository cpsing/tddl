package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.chooser.IndexChooser;
import com.taobao.tddl.optimizer.utils.FilterUtils;

public class FilterSpliterTest extends BaseOptimizerTest {

    @Test
    public void test_主键索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID = 1");
        build(table);

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);
        Assert.assertEquals("TABLE1.ID = 1", result.get(FilterType.IndexQueryKeyFilter).toString());
        Assert.assertEquals(null, result.get(FilterType.IndexQueryValueFilter));
        Assert.assertEquals(null, result.get(FilterType.ResultFilter));
    }

    @Test
    public void test_主键索引条件_特殊条件不走索引() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID != 1 AND ID IS NULL AND ID IS NOT NULL AND ID LIKE '%A' AND ID = 2");
        build(table);

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);
        Assert.assertEquals("TABLE1.ID = 2", result.get(FilterType.IndexQueryKeyFilter).toString());
        Assert.assertEquals("(TABLE1.ID != 1 AND TABLE1.ID IS NULL AND TABLE1.ID IS NOT NULL AND TABLE1.ID LIKE %A)",
            result.get(FilterType.IndexQueryValueFilter).toString());
        Assert.assertEquals(null, result.get(FilterType.ResultFilter));
    }

    @Test
    public void test_二级索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");
        build(table);

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);
        Assert.assertEquals("TABLE1.NAME = 1", result.get(FilterType.IndexQueryKeyFilter).toString());
        Assert.assertEquals(null, result.get(FilterType.IndexQueryValueFilter));
        Assert.assertEquals(null, result.get(FilterType.ResultFilter));
    }

    @Test
    public void test_混合索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1 AND SCHOOL = 2 AND ID > 3");
        build(table); // 会选择NAME做索引，因为=的选择度高，优先选择

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);
        Assert.assertEquals("TABLE1.NAME = 1", result.get(FilterType.IndexQueryKeyFilter).toString());
        Assert.assertEquals("TABLE1.ID > 3", result.get(FilterType.IndexQueryValueFilter).toString());
        Assert.assertEquals("TABLE1.SCHOOL = 2", result.get(FilterType.ResultFilter).toString());
    }

    @Test
    public void test_无索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL = 2");
        build(table); // 无索引，默认选择主键做遍历

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);
        Assert.assertEquals(null, result.get(FilterType.IndexQueryKeyFilter));
        Assert.assertEquals("TABLE1.SCHOOL = 2", result.get(FilterType.IndexQueryValueFilter).toString());
        Assert.assertEquals(null, result.get(FilterType.ResultFilter));
    }

    private void build(TableNode table) {
        table.build();

        Map<String, Object> extraCmd = new HashMap<String, Object>();
        extraCmd.put(ExtraCmd.CHOOSE_INDEX, true);
        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            new ArrayList<ISelectable>(),
            FilterUtils.toDNFNode(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        table.useIndex(index);
    }
}
