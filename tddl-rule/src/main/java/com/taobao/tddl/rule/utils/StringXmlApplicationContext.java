package com.taobao.tddl.rule.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * 字符串形式的Spring ApplicationContext实现。 支持动态订阅spring配置的处理
 * 
 * @author linxuan
 */
public class StringXmlApplicationContext extends AbstractXmlApplicationContext {

    private Resource[]  configResources;
    private ClassLoader cl;

    public StringXmlApplicationContext(String stringXml){
        this(new String[] { stringXml }, null, null);
    }

    public StringXmlApplicationContext(String[] stringXmls){
        this(stringXmls, null, null);
    }

    public StringXmlApplicationContext(String stringXml, ClassLoader cl){
        this(new String[] { stringXml }, null, cl);
    }

    public StringXmlApplicationContext(String[] stringXmls, ClassLoader cl){
        this(stringXmls, null, cl);
    }

    public StringXmlApplicationContext(String[] stringXmls, ApplicationContext parent, ClassLoader cl){
        super(parent);
        this.cl = cl;
        this.configResources = new Resource[stringXmls.length];
        for (int i = 0; i < stringXmls.length; i++) {
            this.configResources[i] = new ByteArrayResource(stringXmls[i].getBytes());
        }
        refresh();
    }

    protected Resource[] getConfigResources() {
        return this.configResources;
    }

    public ClassLoader getClassLoader() {
        if (cl == null) {
            return super.getClassLoader();
        } else {
            return this.cl;
        }
    }
}
