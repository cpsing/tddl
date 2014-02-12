package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.chooser.IndexChooser;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author Dreamond
 */
public class IndexChooserTest extends BaseOptimizerTest {

    private Map               extraCmd     = new HashMap();
    private List<ISelectable> emptyColumns = new ArrayList<ISelectable>();

    @Before
    public void setUp() {
        extraCmd.put(ExtraCmd.CHOOSE_INDEX, true);
    }

    @Test
    public void testChooseIndex() throws QueryException {
        TableNode table = new TableNode("TABLE1");
        QueryTreeNode qn = table.query("ID=1");
        qn.build();

        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            emptyColumns,
            toDNFFilter(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE1");
    }

    /**
     * NAME=1的选择性显然比SCHOOL>1好，所以选择二级索引NAME
     * 
     * @throws QueryException
     */
    @Test
    public void testChooseIndex列出现的顺序不影响索引选择() throws QueryException {
        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qn1 = table1.query("SCHOOL>1&&NAME=1");
        qn1.build();

        IndexMeta index = IndexChooser.findBestIndex(table1.getTableMeta(),
            table1.getColumnsRefered(),
            toDNFFilter(table1.getWhereFilter()),
            table1.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE1._NAME");

        TableNode table2 = new TableNode("TABLE1");
        QueryTreeNode qn2 = table2.query("NAME=1&&SCHOOL>1");
        qn2.build();

        index = IndexChooser.findBestIndex(table2.getTableMeta(),
            table2.getColumnsRefered(),
            toDNFFilter(table2.getWhereFilter()),
            table2.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE1._NAME");
    }

    /**
     * 虽然C1，C2上存在组合索引，但是由于范围查询的选择度不如等值查询 因此还是选择了单索引NAME=1
     */
    @Test
    public void testChooseIndex单索引选择度好于组合索引() throws QueryException {
        TableNode table = new TableNode("TABLE9");
        QueryTreeNode qn = table.query("C1>10&&C2>3&&NAME=1");
        qn.build();

        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            table.getColumnsRefered(),
            toDNFFilter(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE9._NAME");
    }

    /**
     * 虽然C1，C2上都存在单索引，但是C1，C2还是组合索引，这种情况下优先选择组合索引
     * 
     * @throws QueryException
     */
    @Test
    public void testChooseIndex选择组合索引() throws QueryException {
        TableNode table = new TableNode("TABLE9");
        QueryTreeNode qn = table.query("C1>10&&C2=3");
        qn.build();

        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            table.getColumnsRefered(),
            toDNFFilter(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE9._C1_C2");
    }

    /**
     * C4,C5上只存在倒排索引，单C4的选择更高，选择C4倒排
     */
    @Test
    public void testChooseIndex选择倒排索引() throws QueryException {
        TableNode table = new TableNode("TABLE9");
        QueryTreeNode qn = table.query("C4=10&&C5>3");
        qn.build();

        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            table.getColumnsRefered(),
            toDNFFilter(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE9._C4");
    }

    /**
     * C6,C7同时存在组合索引和倒排索引 同时有倒排和组合索引，并且选择度一样，优先选择组合
     */
    @Test
    public void testChooseIndex选择度相同优先选组合() throws QueryException {
        TableNode table = new TableNode("TABLE9");
        QueryTreeNode qn = table.query("C6=10&&C7=3");
        qn.build();

        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            table.getColumnsRefered(),
            toDNFFilter(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        Assert.assertNotNull(index);
        Assert.assertEquals(index.getName(), "TABLE9._C6_C7");
    }

    @Test
    public void testChooseIndex手动指定索引() throws QueryException {
        TableNode table = new TableNode("TABLE9");
        table.build();
        table.useIndex(table.getTableMeta().getIndexs().get(0));
        table.build();
        System.out.println(table.toDataNodeExecutor());
    }

    private List<IFilter> toDNFFilter(IFilter where) {
        return FilterUtils.toDNFNodesArray(where).get(0);
    }
}
