package com.taobao.tddl.monitor.logger;

import java.io.File;

/**
 * 动态增加logger的抽象接口，运行时动态选择子类进行使用
 * 
 * @author jianghang 2013-10-24 下午6:21:07
 * @since 5.0.0
 */
public abstract class DynamicLogger {

    private String encode = "UTF-8";

    protected static String getLogPath() {
        String userHome = System.getProperty("user.home");
        if (!userHome.endsWith(File.separator)) {
            userHome += File.separator;
        }
        String path = userHome + "logs" + File.separator + "tddl" + File.separator;
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return path;
    }

    protected String getEncoding() {
        return encode != null ? encode : System.getProperty("file.encoding", "UTF-8");
    }

    public abstract void init();

    public abstract void initRule();

    public void setEncode(String encode) {
        this.encode = encode;
    }
}
