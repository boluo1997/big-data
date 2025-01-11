package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.compress.utils.Charsets;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author chao
 * @datetime 2025-01-11 17:02
 * @description
 */
public class HttpUtils {

    private static final CloseableHttpClient http;
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(3);
        cm.setDefaultMaxPerRoute(2);
        http = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
    }

    public static String params(Map<String, String> map) {
        return map.entrySet().stream()
                .map(entry -> {
                    try {
                        return entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                        return entry.getKey() + "=" + entry.getValue();
                    }
                })
                .collect(Collectors.joining("&"));
    }


    public static JsonNode apiRequest(String method, String uri, HashMap<String, String> headers, JsonNode body) {
        RequestBuilder requestBuilder = RequestBuilder.create(method)
                .setUri(uri)
                .setHeader("Content-Type", "application/json")
                .setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69");

        Optional.ofNullable(headers).ifPresent(i -> {
            headers.forEach(requestBuilder::setHeader);
        });

        Optional.ofNullable(body).ifPresent(i -> {
            requestBuilder.setEntity(new StringEntity(body.toString(), Charsets.UTF_8));
        });

        try (CloseableHttpResponse response = http.execute(requestBuilder.build()); InputStream is = response.getEntity().getContent()) {
            Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response.getStatusLine());
            return mapper.readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("%s %s:%s", method, uri, body), e);
        }
    }

    public static JsonNode postHttp(String url, JsonNode data) {

        RequestBuilder builder = RequestBuilder.post()
                .setUri(url)
                .setHeader("User-Agent", "Mozilla/5.0")
                .setEntity(new ByteArrayEntity(data.toString().getBytes(), ContentType.APPLICATION_JSON));

        HttpUriRequest req = builder.build();
        try (CloseableHttpResponse res = http.execute(req)) {
            Preconditions.checkArgument(res.getStatusLine().getStatusCode() == 200, res);
            Thread.sleep(100);
            try (InputStream is = res.getEntity().getContent()) {
                return mapper.readTree(is);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
