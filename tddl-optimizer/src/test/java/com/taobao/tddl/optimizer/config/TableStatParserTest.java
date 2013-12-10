package com.taobao.tddl.optimizer.config;

import java.util.List;

import org.junit.Test;

import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.parse.TableIndexStatParser;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.parse.TableStatParser;

public class TableStatParserTest {

    @Test
    public void testSimple() {
        List<TableStat> tableStats = TableStatParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("config/table_stat.xml"));
        System.out.println(tableStats);

        List<TableIndexStat> tableIndexStats = TableIndexStatParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("config/kvIndex_stat.xml"));
        System.out.println(tableIndexStats);
    }
}
