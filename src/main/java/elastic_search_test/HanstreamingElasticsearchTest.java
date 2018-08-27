package elastic_search_test;

import elastic_search_test.config.ElasticsearchConnection;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        GetResponse response = connection.client().prepareGet()
                .setIndex("alarm_20180421")
                .setType("anomaly_scenarios")
                .setId("AWOk_ceVqoKXGlv_HxbT")
                .get();
        String data = response.getSourceAsString();
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
                .setSize(10)
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
            System.out.println(bucket.getKey());
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
}
