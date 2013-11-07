package com.taobao.tddl.rule;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.taobao.tddl.rule.utils.StringXmlApplicationContext;

public class RuleCompatibleHelperTest {

    @Test
    public void testCompatible_interact() {
        try {
            Resource resource = new PathMatchingResourcePatternResolver().getResource("classpath:compatible/interact-spring-context.xml");
            String ruleStr = StringUtils.join(IOUtils.readLines(resource.getInputStream()), SystemUtils.LINE_SEPARATOR);
            ApplicationContext context = new StringXmlApplicationContext(RuleCompatibleHelper.compatibleRule(ruleStr));
            VirtualTableRoot vtr1 = (VirtualTableRoot) context.getBean("vtabroot");
            Assert.assertNotNull(vtr1);
        } catch (IOException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }
    }

    @Test
    public void testCompatible_le() {
        try {
            Resource resource = new PathMatchingResourcePatternResolver().getResource("classpath:compatible/le-spring-context.xml");
            String ruleStr = StringUtils.join(IOUtils.readLines(resource.getInputStream()), SystemUtils.LINE_SEPARATOR);
            ApplicationContext context = new StringXmlApplicationContext(RuleCompatibleHelper.compatibleRule(ruleStr));
            VirtualTableRoot vtr1 = (VirtualTableRoot) context.getBean("vtabroot");
            Assert.assertNotNull(vtr1);
        } catch (IOException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }
    }
}
