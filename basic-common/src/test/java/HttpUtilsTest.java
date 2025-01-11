import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;
import utils.HttpUtils;

import javax.ws.rs.HttpMethod;
import java.util.HashMap;

/**
 * @author chao
 * @datetime 2025-01-11 21:30
 * @description
 */
public class HttpUtilsTest {

    public static final String bossUrl;
    public static final String cookie;
    public static final HashMap<String, String> headers;

    static {
        bossUrl = "https://www.zhipin.com/wapi/zpgeek/search/joblist.json";
        cookie = "ab_guid=ae03803b-0684-436f-ba94-e64f4f5ee1b5; lastCity=101210100; __g=-; __l=l=%2Fwww.zhipin.com%2Fhangzhou%2F%3FseoRefer%3Dindex&r=&g=&s=3&friend_source=0; Hm_lvt_194df3105ad7148dcf2b98a91b5e727a=1735521599,1735548745,1736568378,1736602537; Hm_lpvt_194df3105ad7148dcf2b98a91b5e727a=1736602537; HMACCOUNT=38C4B3764DD960F6; __c=1736602537; __a=52310237.1734520153.1736568378.1736602537.58.16.4.58; __zp_stoken__=babbfw4rDvsOmH0FbZmVpE2x8fsOEc8KCwoDDg33DgcOIwrVWwqfCvGLDiFFUw4JKwopZwrdlwqRywp3CtlJTwptXwrVKwqNbxIZYwozCvcSDSsKmxKfDuMKdw5vCkMOOw4fCrU40FRcLHRcYChYgChIgER8VHx0ZFx0gEh4YEkgywopIPEdQOzNeVF4VV29uV3BTEWhWTE07EhkUHTszRztNR8ONR8OHdcOPRcK7c8OFR8OBw4c7RUdFw4VHQDHDhsORH8OOLhHCvFMZaxHDh8KZcMOXZMOFXz3CvMOwM0FPw4fEvFBII0lGPEJOPE08SDM8csOXYsOPUzXCvMOZPkcmREhOTUg8SE5DRk4sTk9zNkhNN0IeGBYfHTVNw4nDj8OFw69ITg%3D%3D";
        headers = new HashMap<String, String>() {{
            put("cookie", cookie);
        }};
    }


    @Test
    public void apiRequestTest() {
        JsonNode responseNode = HttpUtils.apiRequest(HttpMethod.GET, bossUrl, headers, null);
        Assert.assertEquals(0, responseNode.at("/code").asInt());
    }


}
