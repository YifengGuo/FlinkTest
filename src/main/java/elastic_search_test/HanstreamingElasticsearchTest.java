package elastic_search_test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import elastic_search_test.config.ElasticsearchConnection;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.net.UnknownHostException;
import java.util.*;

import static elastic_search_test.config.DatasourceConstant.*;

/**
 * Created by guoyifeng on 8/24/18
 */
public class HanstreamingElasticsearchTest {
    private ElasticsearchConnection connection;
    private Logger logger = LoggerFactory.getLogger(HanstreamingElasticsearchTest.class);

    @Before
    public void initializeConnection() {
        try {
            connection = new ElasticsearchConnection();
            connection.connect(ES_CLUSTER_NAME, ES_HOSTS, ES_PORT);
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * GetResponse return single document
     */
    @Test
    public void querySingleDocument() {
        SearchResponse response = connection.client().prepareSearch(SCENARIO_SETTING_INDEX)
//                .set()
                .setTypes(SCENARIO_SETTING_TYPE)
                .setQuery(QueryBuilders.termQuery("name", "taikang"))
//                .setId("AWOk_ceVqoKXGlv_HxbT")
                .get();
        String data = response.getHits().getHits()[0].getSourceAsString();
        System.out.println("single document query: " + data);
    }

    @Test
    public void multiQuery() {

        /**
         * difference between match and term:
         *     1. term is fixed whole word search
         *     2. match will firstly split the word by IK split (like N-Gram)
         *        and try to search from indices by each part of the word
         */
        QueryBuilder qb = QueryBuilders.termQuery("user_name", "huanghx09");
        SearchResponse response = connection.client().prepareSearch()
                .setIndices(DATASOURCE_SETTING_INDEX)
                .setTypes(DATASOURCE_SETTING_TYPE)
                .setQuery(qb)
                .execute()
                .actionGet(); // SearchResponse will return 10 records by default
        long count = 0;
        // response.getHits() returns a SearchHits object
        // SearchHits.getHits returns a SearchHit[]
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println("Hit" + count++ + ": " + hit.getSourceAsString());
        }
    }

    @Test
    public void boolQuery() {
        SearchRequestBuilder request = connection.client().prepareSearch("saas_*").setTypes("email");
        QueryBuilder qb = QueryBuilders.boolQuery();
        ((BoolQueryBuilder) qb).must(QueryBuilders.matchQuery("role", "Technician"));
        ((BoolQueryBuilder) qb).mustNot(QueryBuilders.matchQuery("location", "镇江"));
        SearchResponse response = request
                .setQuery(qb)
                // .setFrom(100) // form xxx index to start search
                .setScroll(new TimeValue(1000))  // If set, will enable scrolling of the search request for the specified timeout.
                .setSize(100) // count of result returned by each search
                .addSort("size", SortOrder.DESC)
                .execute()
                .actionGet();
        long count = 0;

        // keep searching till the end
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println("Hit" + count++ + ": " + hit.getSourceAsString());
            }
            response = connection.client().prepareSearchScroll(response.getScrollId()) // continue to scroll from previous position
                    .setScroll(new TimeValue(1000))
                    .execute()
                    .actionGet();
        } while (response.getHits().getHits().length != 0);
    }

    /**
     * Aggregation on some field is like (do something) from table group by xxx in sql
     * goal: get average email size of all engineers (software engineer, electrical engineer)
     * wildcardQuery + TermsBuilder
     */
    @Test
    public void aggregateQuery() {
//        QueryBuilder qb = QueryBuilders.wildcardQuery("role", "*");
        QueryBuilder qb = QueryBuilders.matchAllQuery();
//        AvgBuilder avgBuilder = AggregationBuilders.avg("avgSize").field("size");

        // TermsBuilder is to set a certain field to do the aggregation
        TermsBuilder termsBuilder = AggregationBuilders.terms("roleType").field("role");

        AvgBuilder avgBuilder = AggregationBuilders.avg("avgOccurTime").field("occur_time");

        SearchResponse response = connection.client().prepareSearch("saas_*").setTypes("email")
                .setQuery(qb)
                .addAggregation(termsBuilder)
                .addAggregation(avgBuilder)
                .setScroll(new TimeValue(100))
                .setSize(10000)
                .execute()
                .actionGet();

        // aggMap stores all the aggregation results
        Map<String, Aggregation> aggMap = response.getAggregations().asMap();

        // aggregate on role field to count all the different role types
        StringTerms roleTypeAgg = (StringTerms) aggMap.get("roleType");


        // get average occur_time from all the records using InternalAvg
        InternalAvg occurTimeAvg = (InternalAvg) aggMap.get("avgOccurTime");
        System.out.println("average occur time:");
        System.out.println(new Date((long)occurTimeAvg.getValue()));
        System.out.println();

        System.out.println("all the role types:");
        Iterator<Terms.Bucket> roleIt = roleTypeAgg.getBuckets().iterator();
        while (roleIt.hasNext()) {
            Terms.Bucket bucket = roleIt.next();
            System.out.println(bucket.getKey() + " " + bucket.getDocCount());
        }
        System.out.println();

    }

    /**
     * only retrieve chosen field data
     * use setFetchSource(String[] includes, String[] excludes) to declare which fields to choose and which ones to ignore
     */
    @Test
    public void certainFieldFetch() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse response = connection.client().prepareSearch("saas_*").setTypes("email")
                .setQuery(qb)
                .setFetchSource(new String[]{"size"}, null)
                .setScroll(new TimeValue(1000))
                .setSize(100)
                .execute()
                .actionGet();

        long count  = 0;
        List<Long> sizeArr = new ArrayList<>();
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                // {"size":"21644"}
                String data = hit.getSourceAsString();
                long size = Long.parseLong(data.replaceAll("[{, }, \"]", "").split(":")[1]);
//                long size = Long.valuef(data.replace("\"", "0").split(":")[1]);
                // System.out.println("hit" + count++ + " " + hit.getSourceAsString());
                System.out.println(size);
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(1000))
                    .execute()
                    .actionGet();

        } while (response.getHits().getHits().length != 0);
    }

    @Test
    public void testMatchallQuery() {
        // single query only retrieve data whose size == count in setSize()
        // if we need to fetch all the documents, use while loop to get so.
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse response = connection.client().prepareSearch("saas_*").setTypes("email")
                .setQuery(qb)
                .setFetchSource(new String[]{"role"}, null)
                .setScroll(new TimeValue(1000))
                .setSize(100)
                .execute()
                .actionGet();
        long count  = 0;
        for (SearchHit hit : response.getHits().getHits()) {
            String data = hit.getSourceAsString();
            System.out.println("hit" + count++ + " " + hit.getSourceAsString());
            // System.out.println(size);
        }

        // what if we add an aggregation
        TermsBuilder roleAgg = AggregationBuilders.terms("roleType").field("role");
        SearchResponse response2 = connection.client().prepareSearch("saas_*").setTypes("email")
                .setQuery(qb)
                .setFetchSource(new String[]{"role"}, null)
                .addAggregation(roleAgg)
                .setScroll(new TimeValue(1000))
                .setSize(100)
                .execute()
                .actionGet();
        Map<String, Aggregation> map = response2.getAggregations().asMap();
        StringTerms roles = (StringTerms) map.get("roleType");
        for (Terms.Bucket term : roles.getBuckets()) {
            System.out.println(term.getKey() + " " + term.getDocCount());
        }

        // so if we are aggregating on some field, the aggregation process happens on
        // all the documents of given index/type
    }

    @Test
    public void getAnomalyScenarios() {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.termQuery("scenario_id", "AWOk_sh5qoKXGlv_Hxfk"));
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.existsQuery("scenario"));
        SearchResponse updateSearchResponse = connection.client().prepareSearch(UEBA_ALARM_INDEX).setTypes(ANOMALY_SCENARIOS)
                .setQuery(queryBuilder)
                .setSize(10000)
                .setScroll(new TimeValue(1000))
                .execute().actionGet();

        long count = 0;
        do {
            for (SearchHit hit : updateSearchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
//                String indexId = hit.getId();
//                // update new scenario name one by one
//                UpdateRequest updateRequest = new UpdateRequest().index(UEBA_ALARM_INDEX)
//                        .type(ANOMALY_SCENARIOS)
//                        .id(indexId)
//                        .doc("scenario", scenarioName);
//                LOG.info("scenario_name " + count++ + " " + "{}, id is {}", scenarioName, indexId);
//                connection.client().update(updateRequest);
            }
            updateSearchResponse = connection.client().prepareSearchScroll(updateSearchResponse.getScrollId())
                    .setScroll(new TimeValue(1000))
                    .execute().actionGet();

        } while (updateSearchResponse.getHits().getHits().length != 0);
    }

    @Test
    public void recoverDataFrom31() throws Exception {
        // first get clean data from 31 port and when traversing on each hit
        // initialize new connection to 172 and update its source with source of 31 hit
        // after that data in 172 are almost clean except field scenario is dirty.
        // then get data from 172 and directly update scenario which is "User after hour logon111" to ""
        connection.connect(ES_CLUSTER_NAME, "172.16.150.172", ES_PORT);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.termQuery("scenario_id", "AWOk_sh5qoKXGlv_Hxfk"));
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.termQuery("scenario", ""));
        // ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.existsQuery("scenario"));
        SearchResponse updateSearchResponse = connection.client().prepareSearch(UEBA_ALARM_INDEX).setTypes(ANOMALY_SCENARIOS)
                .setQuery(queryBuilder)
                .setSize(10000)
                .setScroll(new TimeValue(1000))
                .execute().actionGet();
//        FileWriter fw = new FileWriter("/Users/guoyifeng/IdeaProjects/FlinkTest/src/main/java/elastic_search_test/output/whole_scenario_from_31.txt");
        for (SearchHit hit : updateSearchResponse.getHits().getHits()) {
//            fw.write(hit.getSourceAsString() + "\n");
            String indexId = hit.getId();
//            ElasticsearchConnection connectionCurr = new ElasticsearchConnection();
//            connectionCurr.connect(ES_CLUSTER_NAME, "172.16.150.172", 29300);
            UpdateRequest updateRequest = new UpdateRequest().index(UEBA_ALARM_INDEX)
                            .type(ANOMALY_SCENARIOS)
                            .id(indexId)
                            .doc("scenario", null);
            connection.client().update(updateRequest);
            // connection.close();
            System.out.println(hit.getSourceAsString());
        }
//        fw.close();
    }

    @Test
    public void test172Data() throws Exception {
        connection.connect(ES_CLUSTER_NAME, "172.16.150.149", ES_PORT);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.termQuery("scenario_id", "AWOk_sh5qoKXGlv_Hxfk"));
        ((BoolQueryBuilder) queryBuilder).must(QueryBuilders.existsQuery("scenario"));
        SearchResponse updateSearchResponse = connection.client().prepareSearch(UEBA_ALARM_INDEX).setTypes(ANOMALY_SCENARIOS)
                .setQuery(queryBuilder)
                .setSize(10000)
                .setScroll(new TimeValue(1000))
                .execute().actionGet();

        for (SearchHit hit : updateSearchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    @Test
    public void fromMapRefactor() {
        SearchResponse response = connection.client().prepareSearch("ueba_settings")
                .setTypes("user_info")
                .setSize(1000)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery()).get();

        JSONObject res = new JSONObject();
        JSONArray data = new JSONArray();
        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> sourceMap = hit.getSource();
                JSONObject curr = new JSONObject(sourceMap);
                data.add(curr);
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000)).get();

            if (response.getHits().getHits().length == 0) {
                break;
            }
        }

        res.put("data", data);
        res.put("total", response.getHits().getTotalHits());

        data.stream().forEach(o -> System.out.println(o));
    }

    @After
    public void close() {
        connection.close();
    }


}
