/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-5-18)
 */
package com.alibaba.cobar.parser.recognizer.mysql.syntax;

import java.sql.SQLSyntaxErrorException;

import org.junit.Assert;

import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.recognizer.mysql.MySQLToken;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLDMLDeleteParserTest extends AbstractSyntaxTest {

    public void testDelete1() throws SQLSyntaxErrorException {
        String sql = "deLetE LOW_PRIORITY from id1.id , id using t1 a where col1 =? ";
        MySQLLexer lexer = new MySQLLexer(sql);
        MySQLDMLDeleteParser parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        DMLDeleteStatement delete = parser.delete();
        parser.match(MySQLToken.EOF);
        String output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE LOW_PRIORITY ID1.ID, ID FROM T1 AS A WHERE COL1 = ?", output);

        sql = "deLetE from id1.id  using t1  ";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE ID1.ID FROM T1", output);

        sql = "delete from test limit 1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE FROM TEST LIMIT 0, 1", output);

        sql = "delete from offer.*,wp_image.* using offer a,wp_image b where a.member_id=b.member_id and a.member_id='abc' ";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals(
                            "DELETE OFFER.*, WP_IMAGE.* FROM OFFER AS A, WP_IMAGE AS B WHERE A.MEMBER_ID = B.MEMBER_ID AND A.MEMBER_ID = 'abc'",
                            output);

        sql = "deLetE from id1.id where col1='adf' limit 1,?";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE FROM ID1.ID WHERE COL1 = 'adf' LIMIT 1, ?", output);

        sql = "deLetE from id where col1='adf' ordEr by d liMit ? offset 2";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE FROM ID WHERE COL1 = 'adf' ORDER BY D LIMIT 2, ?", output);

        sql = "deLetE id.* from t1,t2 where col1='adf'            and col2=1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE ID.* FROM T1, T2 WHERE COL1 = 'adf' AND COL2 = 1", output);

        sql = "deLetE id,id.t from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE ID, ID.T FROM T1", output);

        sql = "deLetE from t1 where t1.id1='abc' order by a limit 5";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE FROM T1 WHERE T1.ID1 = 'abc' ORDER BY A LIMIT 0, 5", output);

        sql = "deLetE from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE FROM T1", output);

        sql = "deLetE ignore tb1.*,id1.t from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE IGNORE TB1.*, ID1.T FROM T1", output);

        sql = "deLetE quick tb1.*,id1.t from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLDeleteParser(lexer, new MySQLExprParser(lexer));
        delete = parser.delete();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(delete, sql);
        Assert.assertEquals("DELETE QUICK TB1.*, ID1.T FROM T1", output);
    }
}
