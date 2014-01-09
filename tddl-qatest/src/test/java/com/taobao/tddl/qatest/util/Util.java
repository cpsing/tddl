package com.taobao.tddl.qatest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Comment for Util
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-9-10 下午04:51:59
 */

public class Util {

    static String perfResult = "/home/zhuoxue.yll/perfResult.txt";

    // static String perfResult="C:/perfResult.txt";

    public static void creatTxtFile() throws IOException {
        String filenameTemp = perfResult;
        File filename = new File(filenameTemp);
        if (filename.exists()) {
            filename.delete();
            filename.createNewFile();
        } else {
            filename.createNewFile();
        }
    }

    public static boolean writeTxtFile(String newStr) throws IOException {
        boolean flag = false;
        String filein = newStr + "\r\n";
        String temp = "";

        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;

        FileOutputStream fos = null;
        PrintWriter pw = null;
        try {
            File file = new File(perfResult);
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            StringBuffer buf = new StringBuffer();

            while ((temp = br.readLine()) != null) {
                buf = buf.append(temp);
                buf = buf.append(System.getProperty("line.separator"));
            }
            buf.append(filein);

            fos = new FileOutputStream(file);
            pw = new PrintWriter(fos);
            pw.write(buf.toString().toCharArray());
            pw.flush();
            flag = true;
        } catch (IOException e1) {
            throw e1;
        } finally {
            if (pw != null) {
                pw.close();
            }
            if (fos != null) {
                fos.close();
            }
            if (br != null) {
                br.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        return flag;
    }

    /**
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public static int mysqlUpdateData(String sql, List<Object> param, String ip, String db, String user, String passWord)
                                                                                                                         throws Exception {
        int rs = 0;
        Connection con = getConnection(ip, db, user, passWord);
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(sql);
            if (param == null) {
                rs = ps.executeUpdate();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    if (param.get(i) instanceof java.util.Date) {
                        param.set(i, DateUtil.formatDate((java.util.Date) param.get(i), DateUtil.DATE_FULLHYPHEN));
                    }
                }
                for (int i = 0; i < param.size(); i++) {
                    ps.setObject(i + 1, param.get(i));
                }
                rs = ps.executeUpdate();
            }

        } catch (Exception ex) {
            // throw new DataAccessException(ex);
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (con != null) {
                con.close();
            }
            ps = null;
            con = null;
        }
        return rs;
    }

    /**
     * @return
     */
    protected static Connection getConnection(String ip, String db, String user, String passWord) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://" + ip + "/" + db;
            conn = (Connection) DriverManager.getConnection(url, user, passWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

}
