import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;
import utils.SparkUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;

/**
 * @author chao
 * @datetime 2025-03-31 20:04
 * @description
 */
public class SparkUtilsTest {

    @Test
    public void getSparkSessionTest1() throws IllegalAccessException {

        Class<SparkUtils> sparkUtilsClass = SparkUtils.class;
        Field[] fields = sparkUtilsClass.getDeclaredFields();
        Optional<Field> sparkField = Arrays.stream(fields).filter(i -> i.getName().equals("spark")).findFirst();
        Assert.assertTrue(sparkField.isPresent());

        Field field = sparkField.get();
        field.setAccessible(true);
        Assert.assertEquals(field.getName(), "spark");
        Assert.assertEquals(field.getType(), SparkSession.class);
        Assert.assertNull(field.get(sparkUtilsClass));
    }

    @Test
    public void getSparkSessionTest2() throws IllegalAccessException {

        SparkUtils.initialSpark();

        Class<SparkUtils> sparkUtilsClass = SparkUtils.class;
        Field[] fields = sparkUtilsClass.getDeclaredFields();
        Optional<Field> sparkField = Arrays.stream(fields).filter(i -> i.getName().equals("spark")).findFirst();
        Assert.assertTrue(sparkField.isPresent());

        Field field = sparkField.get();
        field.setAccessible(true);
        Assert.assertEquals(field.getName(), "spark");
        Assert.assertEquals(field.getType(), SparkSession.class);
        Assert.assertNotNull(field.get(sparkUtilsClass));
    }

}
