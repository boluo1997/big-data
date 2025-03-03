package com.boluo.process;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;


/**
 * @author chao
 * @datetime 2025-03-03 21:44
 * @description
 */
public class BaseProcessorTest {

    private static LocalDate date = LocalDate.now();
    private static String res = "";

    @Test
    public void timeFormatTest() {
        date = LocalDate.of(2025, 3, 1);
        res = BaseProcessor.timeFormatFunc(date);
        Assert.assertEquals(res, "3/1");

        date = LocalDate.of(2025, 3, 10);
        res = BaseProcessor.timeFormatFunc(date);
        Assert.assertEquals(res, "3/10");

        date = LocalDate.of(2025, 10, 1);
        res = BaseProcessor.timeFormatFunc(date);
        Assert.assertEquals(res, "10/1");

        date = LocalDate.of(2025, 12, 20);
        res = BaseProcessor.timeFormatFunc(date);
        Assert.assertEquals(res, "12/20");

        date = LocalDate.of(2025, 12, 31);
        res = BaseProcessor.timeFormatFunc(date);
        Assert.assertEquals(res, "12/31");
    }

}
