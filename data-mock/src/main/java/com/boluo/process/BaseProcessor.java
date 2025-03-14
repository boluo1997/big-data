package com.boluo.process;

import com.boluo.config.BatchYamlConfig;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import javax.ws.rs.HttpMethod;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * @author chao
 * @datetime 2025-01-09 22:17
 * @description
 */
public abstract class BaseProcessor implements Processor {

    public void process() {
        List<BatchYamlConfig.Job> jobs = (List<BatchYamlConfig.Job>) getJobs();
        System.out.println("current jobs: " + jobs.toString());
        executeTasks(jobs);
    }

    public abstract List<? extends BatchYamlConfig.Job> getJobs();

    public abstract void executeTasks(List<BatchYamlConfig.Job> jobList);

    protected long ingest(BatchYamlConfig.Job job, String batchId) {
        System.out.println("start ingest...");
        Dataset<Row> sourceDs = getApiInfo(job, batchId);

        BatchYamlConfig.Dest destDB = job.getDest();
        SparkUtils.writeToMySQL(sourceDs, destDB.getJdbcUrl(), job.getDestTable(), destDB.getUsername(), destDB.getPassword());
        return 0;
    }

    private Dataset<Row> getApiInfo(BatchYamlConfig.Job job, String batchId) {
        // Map<String, String> params = HttpUtils.parseParams(job.getParams());
        String apiUrl = job.getSourceApiUrl();
        String apiKey = job.getSourceApiKey();
        String date = timeFormatFunc(LocalDate.now());
        HashMap<String, String> params = new HashMap<String, String>() {{
            put("key", apiKey);
            put("date", date);
        }};

        String url = String.format("%s?%s", apiUrl, utils.HttpUtils.params(params));
        JsonNode responseNode = utils.HttpUtils.apiRequest(HttpMethod.GET, url, null, null);
        System.out.println("response body: \n" + responseNode.toString());
        Dataset<Row> ds = SparkSession.active().sql("select 1")
                .withColumn("raw_msg", expr(String.format("'%s'", responseNode)))
                .withColumn("batch_id", expr(String.format("'%s'", batchId)))
                .withColumn("load_date", current_date())
                .withColumn("created_timestamp", current_timestamp())
                .drop("1");
        ds.show(false);


        // {"reason":"success","result":[{"day":"1/12","date":"618年1月12日","title":"曹武彻举兵反隋，建元通圣","e_id":"492"},{"day":"1/12","date":"1515年1月12日","title":"中国明朝政治家海瑞出生","e_id":"493"},{"day":"1/12","date":"1519年1月12日","title":"神圣罗马帝国皇帝马克西米连一世去世","e_id":"494"},{"day":"1/12","date":"1628年1月12日","title":"法国着名作家夏尔·佩罗出生","e_id":"495"},{"day":"1/12","date":"1665年1月12日","title":"业余数学家之王费马逝世","e_id":"496"},{"day":"1/12","date":"1785年1月12日","title":"法国化学家、物理学家杜隆诞生","e_id":"497"},{"day":"1/12","date":"1791年1月12日","title":"四大徽班进北京，京剧开始形成","e_id":"498"},{"day":"1/12","date":"1848年1月12日","title":"意大利巴勒摩城人民举行起义","e_id":"499"},{"day":"1/12","date":"1853年1月12日","title":"太平天国攻占武昌","e_id":"500"},{"day":"1/12","date":"1866年1月12日","title":"大英帝国航空学会成立","e_id":"501"},{"day":"1/12","date":"1869年1月12日","title":"章太炎诞辰","e_id":"502"},{"day":"1/12","date":"1875年1月12日","title":"清同治帝载淳病逝","e_id":"503"},{"day":"1/12","date":"1876年1月12日","title":"美国作家杰克·伦敦诞辰","e_id":"504"},{"day":"1/12","date":"1877年1月12日","title":"形意拳、八卦掌大师韩慕侠出生","e_id":"505"},{"day":"1/12","date":"1884年1月12日","title":"中国首位飞机设计师冯如出生","e_id":"506"},{"day":"1/12","date":"1892年1月12日","title":"台湾第一位加入同盟会的翁俊明出生","e_id":"507"},{"day":"1/12","date":"1893年1月12日","title":"德国战犯赫尔曼·戈林出生","e_id":"508"},{"day":"1/12","date":"1893年1月12日","title":"德国纳粹著名领袖阿尔弗莱德·罗森堡出生","e_id":"509"},{"day":"1/12","date":"1896年1月12日","title":"康有为在上海创办《强学报》","e_id":"510"},{"day":"1/12","date":"1899年1月12日","title":"瑞士化学家保罗·赫尔曼·穆勒出生","e_id":"511"},{"day":"1/12","date":"1903年1月12日","title":"苏联原子弹之父库尔恰托夫出生","e_id":"512"},{"day":"1/12","date":"1904年1月12日","title":"清廷兴修京师观象台","e_id":"513"},{"day":"1/12","date":"1909年1月12日","title":"四维时空理论的创立者德国数学家赫尔曼•闵可夫斯基逝世","e_id":"514"},{"day":"1/12","date":"1912年1月12日","title":"彭家珍与良弼同归于尽","e_id":"515"},{"day":"1/12","date":"1913年1月12日","title":"杭达亲王到俄国，答谢俄承认外蒙古自治","e_id":"516"},{"day":"1/12","date":"1920年1月12日","title":"北京政府教育部废止文言教科书","e_id":"517"},{"day":"1/12","date":"1922年1月12日","title":"香港海员大罢工","e_id":"518"},{"day":"1/12","date":"1926年1月12日","title":"巴斯德研究所称已发现抗破伤风血清","e_id":"519"},{"day":"1/12","date":"1937年1月12日","title":"红军西路军遭受国民党“围剿”","e_id":"520"},{"day":"1/12","date":"1945年1月12日","title":"第二次世界大战苏军进攻到据柏林50公里的奥得河沿线","e_id":"521"},{"day":"1/12","date":"1947年1月12日","title":"刘胡兰英勇就义","e_id":"522"},{"day":"1/12","date":"1949年1月12日","title":"第一次中东战争结束","e_id":"523"},{"day":"1/12","date":"1950年1月12日","title":"苏联恢复战后已废除的死刑适用于间谍、叛国和颠覆政府罪","e_id":"524"},{"day":"1/12","date":"1964年1月12日","title":"中国人民坚决支持巴拿马人民的爱国正义斗争","e_id":"525"},{"day":"1/12","date":"1964年1月12日","title":"美国亚马逊公司创办人杰夫·贝佐斯出生","e_id":"526"},{"day":"1/12","date":"1970年1月12日","title":"尼利亚内战结束","e_id":"527"},{"day":"1/12","date":"1972年1月12日","title":"可口可乐公司收回300万听有问题的饮料","e_id":"528"},{"day":"1/12","date":"1976年1月12日","title":"《尼罗河上的惨案》的作者克里斯蒂逝世","e_id":"529"},{"day":"1/12","date":"1980年1月12日","title":"我国科学工作者首次登上南极大陆","e_id":"530"},{"day":"1/12","date":"1980年1月12日","title":"中国捕获世界上第一条活体白鳍豚","e_id":"531"},{"day":"1/12","date":"1983年1月12日","title":"我国与安哥拉建交","e_id":"532"},{"day":"1/12","date":"1993年1月12日","title":"柏林法院中止对昂纳克的审判","e_id":"533"},{"day":"1/12","date":"1994年1月12日","title":"我国与莱索托王国恢复外交关系","e_id":"534"},{"day":"1/12","date":"1995年1月12日","title":"何梁何利设立科学奖励基金","e_id":"535"},{"day":"1/12","date":"1996年1月12日","title":"陕西周原出土西周早期青铜大鼎","e_id":"536"},{"day":"1/12","date":"1996年1月12日","title":"金三角大毒枭昆沙向缅政府军投降","e_id":"537"},{"day":"1/12","date":"2000年1月12日","title":"中国政府首次派出民事警察执行联合国维和任务","e_id":"538"},{"day":"1/12","date":"2001年1月12日","title":"惠普公司创始人之一威廉·休利特逝世","e_id":"539"},{"day":"1/12","date":"2002年1月12日","title":"铁路部分旅客列车票价实行政府指导价听证会","e_id":"540"},{"day":"1/12","date":"2004年1月12日","title":"全国劳动模范斯霞在南京逝世，享年94岁","e_id":"541"},{"day":"1/12","date":"2006年1月12日","title":"麦加朝觐者发生踩踏事件","e_id":"542"},{"day":"1/12","date":"2010年1月12日","title":"海地发生7级地震","e_id":"543"},{"day":"1/12","date":"2010年1月12日","title":"百度被黑客攻击多省市无法访问","e_id":"544"},{"day":"1/12","date":"2014年1月12日","title":"解放军总后勤部副部长谷俊山被查","e_id":"545"},{"day":"1/12","date":"2016年1月12日","title":"百度血友病吧被卖事件始末","e_id":"546"},{"day":"1/12","date":"2020年1月12日","title":"全国首个电子封条正式启用","e_id":"547"},{"day":"1/12","date":"2020年1月12日","title":"《国家监察》首播","e_id":"548"}],"error_code":0}
        // ArrayNode resultNodes = (ArrayNode) responseNode.at("/result");
        // List<ObjectNode> collect = Streams.stream(resultNodes).map(i -> (ObjectNode) i).collect(Collectors.toList());
        // Dataset<Row> ds = SparkUtils.list2Ds(collect);

        return ds;
    }

    public static String timeFormatFunc(LocalDate date) {
        // 日期规则, 格式:月/日 如:1/1,/10/1,12/12 如月或者日小于10,前面无需加0
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d");
        return date.format(formatter);
    }

}

