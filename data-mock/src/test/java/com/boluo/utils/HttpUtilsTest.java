package com.boluo.utils;

import com.boluo.utils.HttpUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * @author chao
 * @datetime 2025-01-13 19:10
 * @description
 */
public class HttpUtilsTest {

    @Test
    public void parseJuHeParamsTest() {
        String params = "date=replace1";
        Map<String, String> resultMap = HttpUtils.parseParams(params);
        Assert.assertFalse(resultMap.isEmpty());
        Assert.assertEquals(resultMap.size(), 1);
        resultMap.clear();

        params = "a=b&c=d";
        resultMap = HttpUtils.parseParams(params);
        Assert.assertFalse(resultMap.isEmpty());
        Assert.assertEquals(resultMap.size(), 2);
        resultMap.clear();

        params = "a=b&c=d&e=f";
        resultMap = HttpUtils.parseParams(params);
        Assert.assertFalse(resultMap.isEmpty());
        Assert.assertEquals(resultMap.size(), 3);
        resultMap.clear();
    }

}
