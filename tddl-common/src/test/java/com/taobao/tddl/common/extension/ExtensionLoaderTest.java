package com.taobao.tddl.common.extension;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.tddl.common.utils.extension.ExtensionLoader;

public class ExtensionLoaderTest {

    @Test
    public void testSimple() {
        PluginService plugin = ExtensionLoader.load(PluginService.class);
        Assert.assertEquals(plugin.getClass(), ExamplePlugin.class);
        Assert.assertEquals(plugin.echo("hello"), "hello");
    }
}
