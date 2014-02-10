package com.taobao.tddl.optimizer;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.repo.mysql.sqlconvertor.SqlMergeNode;

/**
 * @author Dreamond
 */
public class SqlSelectSingleOptimizerTest extends BaseSqlOptimizerTest {

    @Test
    public void testQuerySelectConstant() throws Exception {
        SqlMergeNode node = getMergeNode("select 'avs' as a from STUDENT  where ID = 1");
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (?) as A from student where (STUDENT.ID = ?)", getSql0(node));

    }

    @Test
    public void testQueryCountString() throws Exception {
        SqlMergeNode node = getMergeNode("select count('avs') from STUDENT  where ID = 1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select COUNT(?) from student where (STUDENT.ID = ?)", getSql0(node));
    }

    @Test
    public void testQuerySelectFilter() throws Exception {
        SqlMergeNode node = getMergeNode("select 1+1 from STUDENT  where ID = 1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (?) from student where (STUDENT.ID = ?)", getSql0(node));
    }

    @Test
    public void testQueryAddArg() throws Exception {
        SqlMergeNode node = getMergeNode("select count(1+1) from STUDENT  where ID = 1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select COUNT(?) from student where (STUDENT.ID = ?)", getSql0(node));
    }

    @Test
    public void testQueryFilterArg() throws Exception {
        SqlMergeNode node = getMergeNode("select count(1=1) from STUDENT");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select COUNT(?) from student", getSql0(node));
    }

    @Test
    public void testQueryFilterArgInWhere() throws Exception {
        SqlMergeNode node = getMergeNode("select count(1=1) from STUDENT  where ID = date(1=1)");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select COUNT(?) from student where (STUDENT.ID = DATE(?))", getSql0(node));
    }

    @Test
    public void testQueryFilterInSelect() throws Exception {
        SqlMergeNode node = getMergeNode("select 1=1 from STUDENT");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (?) from student", getSql0(node));
    }

    @Test
    public void testQueryWithDuplicatedColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select id,id as id1,sum(id)  ,sum(id) as sum1 from STUDENT  where name = 1 group by id order by id");
        System.out.println(node);
        Assert.assertEquals(1, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.ID as ID1,SUM(STUDENT.ID),SUM(STUDENT.ID) as SUM1 from student where (STUDENT.NAME = ?) group by STUDENT.ID asc  order by STUDENT.ID asc ",
            getSql0(node));
    }

    @Test
    public void testQueryWithSameColumnTwiceSingleDB() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select id,id from STUDENT  where name = 1");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.ID from student where (STUDENT.NAME = ?)", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select id,id from STUDENT  where name = 1 and id=1");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.ID from student where (STUDENT.ID = ?) and (STUDENT.NAME = ?)",
                getSql0(node));
        }
    }

    @Test
    public void testQueryWithSameColumnTwiceMultiDB() throws Exception {
        try {
            SqlMergeNode node = getMergeNode("select id,id from STUDENT");
            System.out.println(node);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("'STUDENT.ID' is ambiguous"));
        }
    }

    @Test
    public void testQueryWithValueEqualColumn() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select * from STUDENT  where 1=id");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from STUDENT  where 1=name");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME = ?)",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from STUDENT  where 1<id");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID > ?)",
                getSql0(node));
        }
    }

    @Test
    public void testQueryWithFunctionEqualColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select * from STUDENT  where now()=id");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (NOW() = STUDENT.ID)",
            getSql0(node));
    }

    @Test
    public void testQueryWithCompExpr() throws Exception {
        SqlMergeNode node = getMergeNode("select 1>1,1||1,1&1,1=1,1<=>1 from student");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (? > ?),(?),(? & ?),(?),(?) from student", getSql0(node));
    }

    @Test
    public void testQueryWithBetweenInSelect() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT 1 BETWEEN 2 AND 3 from student");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ((? >= ?) AND (? <= ?)) from student", getSql0(node));

    }

    @Test
    public void testQueryWithXOR() throws Exception {
        {
            SqlMergeNode node = getMergeNode("SELECT 1 ^ 1 from STUDENT where  1 + 1 limit 1");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (? ^ ?) from student where (?) limit ?,?", getSql0(node));// bitxor
        }
        {
            SqlMergeNode node = getMergeNode("SELECT 1 xor 1 from STUDENT where 1 ^ 1");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (?) from student where ((? ^ ?))", getSql0(node)); // logicalxor
        }
    }

    @Test
    public void testQueryWithInInSelect() throws Exception {
        SqlMergeNode node = getMergeNode("select  'wefwf' IN (0,3,5,'wefwf') from student where 'wefwf' IN (0,3,5,'wefwf')");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (? IN (?,?,?,?)) from student where (? IN (?,?,?,?))", getSql0(node));
    }

    @Test
    public void testQueryWithJoinAndColumnAndStar() throws Exception {
        SqlMergeNode node = getMergeNode("select *,count(*) from STUDENT s join STUDENT t on s.name = t.name ");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select S.ID,S.NAME,S.SCHOOL,T.ID,T.NAME,T.SCHOOL,COUNT(*) from student S join student T on S.NAME = T.NAME",
            getSql0(node));
    }

    @Test
    public void testQueryWithEqualNull() throws Exception {
        SqlMergeNode node = getMergeNode("select *  from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = null)",
            getSql0(node));
    }

    @Test
    public void testQueryWithNotNull() throws Exception {
        SqlMergeNode node = getMergeNode("select not null from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select NOT(null) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithAndNull() throws Exception {
        SqlMergeNode node = getMergeNode("select 1 && NULL from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ((?) AND (null)) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithIsTrue() throws Exception {
        SqlMergeNode node = getMergeNode("select 1 is true from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ? IS TRUE from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithIsNotTrue() throws Exception {
        SqlMergeNode node = getMergeNode("select 1 is not true from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ? IS NOT TRUE from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithConv() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT CONV(-17,10,-18) from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select CONV(?,?,?) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWith取反结果() throws Exception {
        try {
            SqlMergeNode node = getMergeNode("SELECT !(1+ID) from STUDENT where id = null");
            System.out.println(node);
            Assert.fail();
        } catch (Exception e) {
            // 暂时不支持
        }
    }

    @Test
    public void testQueryWith取反结果_提前计算() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT !(1+1) from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (?) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithKuohao() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT 102/(1-ID) from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (? / (? - STUDENT.ID)) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithInterval() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200) from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select INTERVAL(?,?,?,?,?,?,?) from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithDateInterval() throws Exception {
        {
            SqlMergeNode node = getMergeNode("SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY) from STUDENT where id = null");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select ADDDATE(?,INTERVAL ? DAY) from student where (STUDENT.ID = null)",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("SELECT '2008-12-31 23:59:59' + INTERVAL 1 SECOND from STUDENT where id = null");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (? + INTERVAL ? SECOND) from student where (STUDENT.ID = null)", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("SELECT DATE_ADD('1992-12-31 23:59:59.000002' , INTERVAL '1.999999' SECOND_MICROSECOND)  from STUDENT where id = null");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select DATE_ADD(?,INTERVAL ? SECOND_MICROSECOND) from student where (STUDENT.ID = null)",
                getSql0(node));
        }

    }

    @Test
    public void testQueryWithStringAndFloatCopmare() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT '.01' = 0.01 as T from STUDENT where id = null");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select (?) as T from student where (STUDENT.ID = null)", getSql0(node));
    }

    @Test
    public void testQueryWithIf() throws Exception {
        SqlMergeNode node = getMergeNode("select sum(IF (t.id=-1,1,0))  from STUDENT t where id = 1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select SUM(IF((T.ID = ?),?,?)) from student T where (T.ID = ?)", getSql0(node));
    }

    @Test
    public void testQueryWithOperatorEqualColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select * from STUDENT  where 1+1=id");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
            getSql0(node));
    }

    @Test
    public void testPartitionColumnQuery() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where name=1");
        System.out.println(node);
        Assert.assertEquals(1, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithNullArg() throws Exception {
        {
            SqlMergeNode node = getMergeNode("SELECT ASCII(NULL) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select ASCII(null) from student", getSql0(node));
        }
        {
            SqlMergeNode node = getMergeNode("SELECT BIT_LENGTH(null) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select BIT_LENGTH(null) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("SELECT BIN(null) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select BIN(null) from student", getSql0(node));
        }
        {
            SqlMergeNode node = getMergeNode("SELECT QUOTE(NULL) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select QUOTE(null) from student", getSql0(node));
        }
    }

    @Test
    public void testQueryWithIsNull() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT 1 IS NULL, 0 IS NULL, NULL IS NULL from student");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ? IS NULL,? IS NULL,null IS NULL from student", getSql0(node));
    }

    @Test
    public void testQueryWithSelectNull() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select (null) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (null) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("SELECT null from student where id=null");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (null) from student where (STUDENT.ID = null)", getSql0(node));
        }
    }

    @Test
    public void testQueryWithIsNotNull() throws Exception {
        SqlMergeNode node = getMergeNode("SELECT 1 IS NOT NULL, 0 IS NOT NULL, NULL IS NOT NULL from student");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select ? IS NOT NULL,? IS NOT NULL,null IS NOT NULL from student", getSql0(node));
    }

    @Test
    public void testQueryWithoutCondition() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student", getSql0(node));
    }

    @Test
    public void testQueryWithMinusOperatorInt() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select ID-1, ID--1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (STUDENT.ID - ?),(STUDENT.ID - ?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select ID-1, ID-1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (STUDENT.ID - ?),(STUDENT.ID - ?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select ID-1, ID---1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (STUDENT.ID - ?),(STUDENT.ID - ?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select (ID - 1),(ID - -(-(-(1)))) from STUDENT ");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (STUDENT.ID - ?),(STUDENT.ID - ?) from student", getSql0(node));
        }

    }

    @Test
    public void testQueryWithMinusOperatorFloat() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select 1-1.1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1--1.1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1-1, 1---1.1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (?),(?) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1-1, 1----1.1 from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select (?),(?) from student", getSql0(node));
        }

    }

    @Test
    public void testQueryWithMinusOperatorCount() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select -(id) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select -(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select --id from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select -(-(STUDENT.ID)) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select --date(-id) from student");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select -(-(DATE(-(STUDENT.ID)))) from student", getSql0(node));
        }

    }

    @Test
    public void testQueryWithConditionNotPartitionColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where school=1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.SCHOOL = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithBlob() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where school=_binary'asdasdasdsadsa'");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.SCHOOL = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithBit() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where school=b'101010101010'");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.SCHOOL = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithValueEqualValue() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id=1 and 1=1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
            getSql0(node));

    }

    @Test
    public void testQueryWithTrueEqualTrue() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id=1 and true=true");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
            getSql0(node));

    }

    @Test
    public void testQueryWithTrueAndTrue() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id=1 and true");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithTrueAndFalse() throws Exception {
        try {
            SqlMergeNode node = getMergeNode("select * from student where id=1 and false");
            System.out.println(node);
            Assert.fail();
        } catch (Exception e) {
            // 空结果
            Assert.assertTrue(e.getMessage().contains("空结果"));
        }
    }

    @Test
    public void testQueryWithTrueOrFalse() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id=1 or false");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?)",
            getSql0(node));
    }

    @Test
    public void testQueryWithTrueOrTrue() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id=1 or true");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (?)", getSql0(node));
    }

    @Test
    public void testQueryWithRange() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where id>1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID > ?)",
            getSql0(node));

    }

    @Test
    public void testQueryWithOr() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where name=1 or name='sadasd'");
        System.out.println(node);
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where ((STUDENT.NAME = ?) OR (STUDENT.NAME = ?))",
            getSql0(node));
    }

    @Test
    public void testQueryWithIn() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select * from student where NAME In (1)");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME IN (?))",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student where NAME In (1) and id=1");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID = ?) and (STUDENT.NAME IN (?))",
                getSql0(node));
        }
        {
            SqlMergeNode node = getMergeNode("select * from student where name in (1,2)");
            System.out.println(node);
            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME IN (?,?))",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student where name in (1,2,3,4,5,6,7,8,9,0)");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME IN (?,?,?,?,?,?))",
                getSql0(node));

            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.NAME IN (?,?,?,?))",
                getSql1(node));
        }
    }

    @Test
    public void testQueryWithPartitionColumnAndNotPartitionColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where name=1 and school=1");
        System.out.println(node);
        Assert.assertEquals(1, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where ((STUDENT.NAME = ?) AND (STUDENT.SCHOOL = ?))",
            getSql0(node));

    }

    @Test
    public void testQueryWithPartitionColumnOrNotPartitionColumn() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student where name=1 or school=1");
        System.out.println(node);
        Assert.assertEquals(2, node.getSubQuerys().size());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where ((STUDENT.NAME = ?) OR (STUDENT.SCHOOL = ?))",
            getSql0(node));
    }

    @Test
    public void testQueryWithOrderByPk() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select * from student order by id");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals(1, node.getOrderBy().size());
            Assert.assertEquals("ID", node.getOrderBy().get(0).getColumn().getColumnName());
            Assert.assertEquals((Boolean) true, node.getOrderBy().get(0).getDirection());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student order by STUDENT.ID asc ",
                getSql0(node));

        }
        {
            SqlMergeNode node = getMergeNode("select id as p from student order by p");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals(1, node.getOrderBy().size());
            Assert.assertEquals("P", node.getOrderBy().get(0).getColumn().getColumnName());
            Assert.assertEquals((Boolean) true, node.getOrderBy().get(0).getDirection());
            Assert.assertEquals("select STUDENT.ID as P from student order by STUDENT.ID asc ", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select id as p from student order by id");
            System.out.println(node);
            Assert.assertEquals(2, node.getSubQuerys().size());
            Assert.assertEquals(1, node.getOrderBy().size());
            Assert.assertEquals("P", node.getOrderBy().get(0).getColumn().getColumnName());
            Assert.assertEquals((Boolean) true, node.getOrderBy().get(0).getDirection());
            Assert.assertEquals("select STUDENT.ID as P from student order by STUDENT.ID asc ", getSql0(node));

        }
    }

    @Test
    public void testQueryWithAvgSingleDb() throws Exception {
        SqlMergeNode node = getMergeNode("select avg(id) from student where name=1");
        System.out.println(node);
        // Assert.assertEquals(1, node.getAggs().size());
        // Assert.assertEquals("AVG(id)",
        // node.getAggs().get(0).getColumnName());
        Assert.assertEquals("select AVG(STUDENT.ID) from student where (STUDENT.NAME = ?)", getSql0(node));
    }

    @Test
    public void testQueryWithRollUp() throws Exception {
        try {
            SqlMergeNode node = getMergeNode("select * from student group by id with rollup");
            System.out.println(node);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("with rollup is not supported yet!"));
        }
    }

    @Test
    public void testQueryWithLimitZero() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select * from student");
            System.out.println(node);
            Assert.assertEquals(null, node.getLimitFrom());
            Assert.assertEquals(null, node.getLimitTo());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student limit 0,0");
            System.out.println(node);
            Assert.assertEquals((Long) 0L, node.getLimitFrom());
            Assert.assertEquals((Long) 0L, node.getLimitTo());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student limit ?,?", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student limit 0");
            System.out.println(node);
            Assert.assertEquals((Long) 0L, node.getLimitFrom());
            Assert.assertEquals((Long) 0L, node.getLimitTo());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student limit ?,?", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student limit 15");
            System.out.println(node);
            Assert.assertEquals((Long) 0L, node.getLimitFrom());
            Assert.assertEquals((Long) 15L, node.getLimitTo());
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student limit ?,?", getSql0(node));
        }
    }

    @Test
    public void testQueryWithAvgSingleQuery() throws Exception {
        SqlMergeNode node = getMergeNode("select avg(id) from student where name=1");
        System.out.println(node);
        // Assert.assertEquals(1, node.getAggs().size());
        // Assert.assertEquals("AVG(id)",
        // node.getAggs().get(0).getColumnName());
        Assert.assertEquals("select AVG(STUDENT.ID) from student where (STUDENT.NAME = ?)", getSql0(node));
    }

    @Test
    public void testQueryWithCount() throws Exception {
        SqlMergeNode node = getMergeNode("select count(id) from student");
        System.out.println(node);
        // Assert.assertEquals(1, node.getAggs().size());
        // Assert.assertEquals("COUNT(id)",
        // node.getAggs().get(0).getColumnName());
        Assert.assertEquals("select COUNT(STUDENT.ID) from student", getSql0(node));
        Assert.assertEquals("select COUNT(STUDENT.ID) from student", getSql1(node));

    }

    @Test
    public void testQueryWithMax() throws Exception {
        SqlMergeNode node = getMergeNode("select max(id) from student");
        System.out.println(node);
        // Assert.assertEquals(1, node.getAggs().size());
        // Assert.assertEquals("MAX(id)",
        // node.getAggs().get(0).getColumnName());

        Assert.assertEquals("select MAX(STUDENT.ID) from student", getSql0(node));
        Assert.assertEquals("select MAX(STUDENT.ID) from student", getSql1(node));

    }

    @Test
    public void testQueryWithMin() throws Exception {
        SqlMergeNode node = getMergeNode("select min(id) from student");
        System.out.println(node);
        Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql0(node));
        Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql1(node));

    }

    @Test
    public void testQueryWithSum() throws Exception {
        SqlMergeNode node = getMergeNode("select sum(id) from student");
        System.out.println(node);
        // Assert.assertEquals(1, node.getAggs().size());
        // Assert.assertEquals("SUM(id)",
        // node.getAggs().get(0).getColumnName());
        Assert.assertEquals("select SUM(STUDENT.ID) from student", getSql0(node));

    }

    @Test
    public void testQueryWithScalarFunction() throws Exception {
        SqlMergeNode node = getMergeNode("select date(id) from student");
        System.out.println(node);
        // Assert.assertEquals(0, node.getAggs().size());
        Assert.assertEquals("select DATE(STUDENT.ID) from student", getSql0(node));

    }

    @Test
    public void testQueryWithAggregateFunctionWithGroupBy() throws Exception {
        SqlMergeNode node = getMergeNode("select sum(id),name from student group by name");
        System.out.println(node);
        Assert.assertEquals(1, node.getGroupBys().size());
        Assert.assertEquals("NAME", node.getGroupBys().get(0).getColumnName());
        Assert.assertEquals("select SUM(STUDENT.ID),STUDENT.NAME from student group by STUDENT.NAME asc ",
            getSql0(node));

    }

    @Test
    public void testQueryWithAggregateFunctionWithGroupByOrderBy() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select sum(id),name from student where name=1 group by name order by sum(id)");
            System.out.println(node);
            Assert.assertEquals(1, node.getGroupBys().size());
            Assert.assertEquals("NAME", node.getGroupBys().get(0).getColumnName());
            // Assert.assertEquals(1, node.getAggs().size());
            // Assert.assertEquals("SUM(id)",
            // node.getAggs().get(0).getColumnName());

            Assert.assertEquals(1, node.getSubQuerys().size());
            Assert.assertEquals(1, node.getOrderBy().size());
            Assert.assertEquals("SUM(ID)", node.getOrderBy().get(0).getColumn().getColumnName());

            Assert.assertEquals("select SUM(STUDENT.ID),STUDENT.NAME from student where (STUDENT.NAME = ?) group by STUDENT.NAME asc  order by SUM(STUDENT.ID) asc ",
                getSql0(node));
        }

        // {
        // try {
        // getMergeNode("select sum(id) ,id from student group by id order by sum(id)");
        // Assert.fail("order by and group by is not matched and is not a single group query");
        // } catch (IllegalArgumentException e) {
        //
        // }
        // }
    }

    @Test
    public void testQueryWithLimit() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student limit 5,15");
        System.out.println(node);
        Assert.assertEquals((Long) 5L, (Long) node.getLimitFrom());
        Assert.assertEquals((Long) 15L, (Long) node.getLimitTo());
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student limit ?,?", getSql0(node));
    }

    @Test
    public void testQueryWithAggregateFunctionWithoutGroupBySelected() throws Exception {
        SqlMergeNode node = getMergeNode("select sum(id) from student group by name");
        System.out.println(node);
    }

    @Test
    public void testQueryWithTempColumns() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select school from student order by id");
            System.out.println(node);
            Assert.assertEquals(1, node.getColumns().size());
            Assert.assertEquals("select STUDENT.SCHOOL,STUDENT.ID from student order by STUDENT.ID asc ", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select school from student group by id");
            System.out.println(node);
            Assert.assertEquals(1, node.getColumns().size());
            Assert.assertEquals("select STUDENT.SCHOOL,STUDENT.ID from student group by STUDENT.ID asc ", getSql0(node));
        }
        {
            SqlMergeNode node = getMergeNode("select school from student where name=1 order by id");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.SCHOOL from student where (STUDENT.NAME = ?) order by STUDENT.ID asc ",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select school from student where name=1 group by id");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.SCHOOL from student where (STUDENT.NAME = ?) group by STUDENT.ID asc ",
                getSql0(node));
        }

    }

    @Test
    public void testQueryWithDistinctSingleDB() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select distinct(school) from student where name=1");
            System.out.println(node);
            Assert.assertEquals(1, node.getColumns().size());
            Assert.assertEquals("select  distinct STUDENT.SCHOOL from student where (STUDENT.NAME = ?)", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select count(distinct school) from student where name=1");
            System.out.println(node);
            Assert.assertEquals(1, node.getColumns().size());
            Assert.assertEquals("select COUNT( distinct STUDENT.SCHOOL) from student where (STUDENT.NAME = ?)",
                getSql0(node));
        }
    }

    @Test
    public void testQueryWithGroupByMultiCol() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student group by name,id,school");
        System.out.println(node);
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student group by STUDENT.NAME asc ,STUDENT.ID asc ,STUDENT.SCHOOL asc ",
            getSql0(node));

    }

    @Test
    public void testQueryWithOrderByMultiCol() throws Exception {
        SqlMergeNode node = getMergeNode("select * from student order by name,id,school");
        System.out.println(node);
        Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student order by STUDENT.NAME asc ,STUDENT.ID asc ,STUDENT.SCHOOL asc ",
            getSql0(node));
    }

    @Test
    public void testQueryWithGroupByAndOrderBy() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select SUM(id),NAME from STUDENT  group by NAME asc order by NAME asc");
            System.out.println(node);
            Assert.assertEquals("select SUM(STUDENT.ID),STUDENT.NAME from student group by STUDENT.NAME asc  order by STUDENT.NAME asc ",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select sum(id) ,name from student group by name order by name desc");
            System.out.println(node);
            Assert.assertEquals("select SUM(STUDENT.ID),STUDENT.NAME from student group by STUDENT.NAME desc  order by STUDENT.NAME desc ",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select sum(id) ,name from student group by name order by sum(id) desc");
            System.out.println(node);
            Assert.assertEquals("select SUM(STUDENT.ID),STUDENT.NAME from student group by STUDENT.NAME asc  order by STUDENT.NAME asc ",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select * from student group by name,id order by id,name");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student group by STUDENT.ID asc ,STUDENT.NAME asc  order by STUDENT.ID asc ,STUDENT.NAME asc ",
                getSql0(node));
        }
    }

    @Test
    public void testQueryWithHavingCrossDb() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select * from student where id>1 having id<2");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME,STUDENT.SCHOOL from student where (STUDENT.ID > ?)",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select max(id),name from student group by name having max(id)<2");
            System.out.println(node);
            Assert.assertEquals("select MAX(STUDENT.ID),STUDENT.NAME from student group by STUDENT.NAME asc ",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select max(id) m,name from student group by name having m<2");
            System.out.println(node);
            Assert.assertEquals("select MAX(STUDENT.ID) as M,STUDENT.NAME from student group by STUDENT.NAME asc ",
                getSql0(node));
        }

    }

    @Test
    public void testQueryWithHavingSingleDb() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select max(id) m,name from student where name=2 group by name having m<2");
            System.out.println(node);

            Assert.assertEquals("select MAX(STUDENT.ID) as M,STUDENT.NAME from student where (STUDENT.NAME = ?) group by STUDENT.NAME asc  having (MAX(STUDENT.ID) < ?)",
                getSql0(node));
        }

    }

    @Test
    public void testQueryWithScalarAggregateFunctionSingleDb() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select max(id)+min(id) from student where name=2");
            System.out.println(node);
            Assert.assertEquals("select (MAX(STUDENT.ID) + MIN(STUDENT.ID)) from student where (STUDENT.NAME = ?)",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select -min(id) from student where name=2");
            System.out.println(node);
            Assert.assertEquals("select -(MIN(STUDENT.ID)) from student where (STUDENT.NAME = ?)", node.getSubQuerys()
                .get("group0")
                .get(0)
                .getSql());

        }

    }

    // @Test
    // public void testQueryWithScalarAggregateFunctionMultiDb2() throws
    // Exception {
    // try {
    // SqlMergeNode node = getMergeNode(
    // "SELECT DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'EUR')) from student",
    // schema, null, null);
    // System.out.println(node);
    // Assert.fail();
    // } catch (Exception e) {
    // Assert.assertEquals(
    // "Function using like this: scalar(aggregate()) is not supported for crossing db",
    // e.getMessage());
    // }
    // }
    @Test
    public void testQueryWithScalarAggregateFunctionMultiDb() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select max(id)+min(id) from student");
            System.out.println(node);
            Assert.assertEquals("select MAX(STUDENT.ID),MIN(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select max(id)+1 from student");
            System.out.println(node);
            Assert.assertEquals("select MAX(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1+min(id) from student");
            System.out.println(node);
            Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1*min(id) from student");
            System.out.println(node);
            Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select 1/min(id) from student");
            System.out.println(node);
            Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select -min(id) from student");
            System.out.println(node);
            Assert.assertEquals("select MIN(STUDENT.ID) from student", getSql0(node));
        }
    }

    @Test
    public void testRowInAndEqualsFunction() throws Exception {
        {
            SqlMergeNode node = getMergeNode("select ID,NAME from student where (id,name) in ((1,2),(2,3))");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME from student where ((STUDENT.ID,STUDENT.NAME) IN ((?,?),(?,?)))",
                getSql0(node));
        }

        {
            SqlMergeNode node = getMergeNode("select ID,NAME from student where (id,name) = (1,2)");
            System.out.println(node);
            Assert.assertEquals("select STUDENT.ID,STUDENT.NAME from student where ((STUDENT.ID,STUDENT.NAME) = (?,?))",
                getSql0(node));
        }
    }
}
