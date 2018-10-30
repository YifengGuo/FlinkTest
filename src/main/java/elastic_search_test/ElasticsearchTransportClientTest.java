package elastic_search_test;

import elastic_search_test.config.ElasticsearchConnection;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;

import static elastic_search_test.config.DatasourceConstant.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

/**
 * Created by guoyifeng on 10/21/18.
 */

/**
 * this test class is to test some api's performance of current transport client
 * and will do some comparisons between rest client and transport client
 */
public class ElasticsearchTransportClientTest {
    private ElasticsearchConnection connection;

    private Logger logger = LoggerFactory.getLogger(ElasticsearchTransportClientTest.class);

    @Before
    public void initial() {
        // initialize transport client
        connection = new ElasticsearchConnection();
        try {
            connection.connect(ES_CLUSTER_NAME, ES_HOSTS, ES_PORT);
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }

    }

    /**
     * test scroll search api performance of transport client
     */
    @Test
    public void testSearchPerformance() {
        long startTime = System.currentTimeMillis();
        SearchResponse response = connection.client().prepareSearch(UEBA_SETTINGS_INDEX)
//                .setTypes("user_info")
                .setSize(10000)
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setQuery(QueryBuilders.matchAllQuery())
                .get();

        SearchHit[] hits = response.getHits().getHits();
        int counter = 0;

        while (hits != null && hits.length != 0) {
            for (SearchHit hit : hits) {
//                System.out.println(counter);
                counter++;
            }
            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }
        System.out.println(counter);
        long endTime = System.currentTimeMillis();
        System.out.printf("it takes %f seconds to fetch data in user_info\n", (endTime - startTime) / 1000.0);
    }

    @Test
    public void testAvgAggregation() {
        MetricsAggregationBuilder aggregation = AggregationBuilders
                .avg("agg")
                .field("score");
        SearchRequestBuilder searchRequest = connection.client().prepareSearch()
                .setIndices(UEBA_ALARM_INDEX)
                .setTypes("anomaly_scenarios")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(aggregation);

        SearchResponse response = searchRequest.execute().actionGet();

        Avg agg = response.getAggregations().get("agg");
        Double value = agg.getValue();
        if (value.isNaN()) {
            System.out.println(0);
        } else {
            System.out.println(value.doubleValue());
        }
    }

    @Test
    public void testGroupBy() {
        BoolQueryBuilder qb = boolQuery()
                .filter(rangeQuery("occur_time").from(1530400200000L).to(1539334992074L));

        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("agg").field("user_name").size(10);

        SearchResponse res = connection.client().prepareSearch()
                .setIndices(UEBA_ALARM_INDEX)
                .setTypes("anomaly_scenarios")
                .setQuery(qb)
                .addAggregation(aggregation).get();

        System.out.println(res);
    }

    // cardinality: A single-value metrics aggregation that calculates an approximate count of distinct values
    @Test
    public void testCardinalityNum() {
        String type = "*";
        BoolQueryBuilder qb = boolQuery()
                .filter(rangeQuery("occur_time").from(1530400200000L).to(1539334992074L));
        MetricsAggregationBuilder aggregation = AggregationBuilders.cardinality("agg").field("user_name");
        SearchRequestBuilder searchRequest = connection.client().prepareSearch()
                .setIndices("ueba_alarm")
                .setQuery(qb)
                .addAggregation(aggregation);
        if (!type.equals("*")) {
            searchRequest.setTypes(type);
        }
        SearchResponse response = searchRequest.execute().actionGet();
        Cardinality agg = response.getAggregations().get("agg");

        HashMap<String, Long> responseMap = new HashMap<>();
        responseMap.put("total", response.getHits().getTotalHits());
        responseMap.put("cardinality", agg.getValue());

        responseMap.forEach((k, v) -> {
            System.out.println(k + " " + v);
        });
    }
}
