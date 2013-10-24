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
import java.util.Collections;

import org.junit.Assert;

import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralHexadecimal;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.parser.recognizer.mysql.MySQLToken;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLDMLInsertParserTest extends AbstractSyntaxTest {

    public void testInsertHex() throws SQLSyntaxErrorException {
        String sql = "insert into single_node(username,password,age,num) values(X'4D7953514C',X'4D7953514C',100,100)";
        SQLStatement stmt = SQLParserDelegate.parse(sql);
        DMLInsertStatement insert = (DMLInsertStatement) stmt;
        Assert.assertEquals(1, insert.getRowList().size());
        LiteralHexadecimal hex = (LiteralHexadecimal) insert.getRowList().get(0).getRowExprList().get(0);
        Assert.assertEquals("MySQL", hex.evaluationInternal(Collections.emptyMap()));
    }

    public void testInsert() throws SQLSyntaxErrorException {
        String sql = "insErt HIGH_PRIORITY intO test.t1 seT t1.id1=?, id2 := '123'";
        MySQLLexer lexer = new MySQLLexer(sql);
        MySQLDMLInsertParser parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        DMLInsertStatement insert = parser.insert();
        parser.match(MySQLToken.EOF);
        String output = output2MySQL(insert, sql);
        Assert.assertNotNull(insert);
        Assert.assertEquals("INSERT HIGH_PRIORITY INTO TEST.T1 (T1.ID1, ID2) VALUES (?, '123')", output);

        sql = "insErt  IGNORE test.t1 seT t1.id1:=? oN dupLicatE key UPDATE ex.col1=?, col2=12";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals(
                            "INSERT IGNORE INTO TEST.T1 (T1.ID1) VALUES (?) ON DUPLICATE KEY UPDATE EX.COL1 = ?, COL2 = 12",
                            output);

        sql = "insErt t1 value (123,?) oN dupLicatE key UPDATE ex.col1=?";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT INTO T1 VALUES (123, ?) ON DUPLICATE KEY UPDATE EX.COL1 = ?", output);

        sql = "insErt LOW_PRIORITY t1 valueS (12e-2,1,2), (?),(default)";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT LOW_PRIORITY INTO T1 VALUES (0.12, 1, 2), (?), (DEFAULT)", output);

        sql = "insErt LOW_PRIORITY t1 select id from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT LOW_PRIORITY INTO T1 SELECT ID FROM T1", output);

        sql = "insErt delayed t1 select id from t1";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT DELAYED INTO T1 SELECT ID FROM T1", output);

        sql = "insErt LOW_PRIORITY t1 (select id from t1) oN dupLicatE key UPDATE ex.col1=?, col2=12";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals(
                            "INSERT LOW_PRIORITY INTO T1 SELECT ID FROM T1 ON DUPLICATE KEY UPDATE EX.COL1 = ?, COL2 = 12",
                            output);

        sql = "insErt LOW_PRIORITY t1 (t1.col1) valueS (123),('12''34')";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT LOW_PRIORITY INTO T1 (T1.COL1) VALUES (123), ('12\\'34')", output);

        sql = "insErt LOW_PRIORITY t1 (col1, t1.col2) VALUE (123,'123\\'4') oN dupLicatE key UPDATE ex.col1=?";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals(
                            "INSERT LOW_PRIORITY INTO T1 (COL1, T1.COL2) VALUES (123, '123\\'4') ON DUPLICATE KEY UPDATE EX.COL1 = ?",
                            output);

        sql = "insErt LOW_PRIORITY t1 (col1, t1.col2) select id from t3 oN dupLicatE key UPDATE ex.col1=?";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals(
                            "INSERT LOW_PRIORITY INTO T1 (COL1, T1.COL2) SELECT ID FROM T3 ON DUPLICATE KEY UPDATE EX.COL1 = ?",
                            output);

        sql = "insErt LOW_PRIORITY IGNORE intO t1 (col1) ( select id from t3) ";
        lexer = new MySQLLexer(sql);
        parser = new MySQLDMLInsertParser(lexer, new MySQLExprParser(lexer));
        insert = parser.insert();
        parser.match(MySQLToken.EOF);
        output = output2MySQL(insert, sql);
        Assert.assertEquals("INSERT LOW_PRIORITY IGNORE INTO T1 (COL1) SELECT ID FROM T3", output);

    }
}
