package elastic_search_test;

import elastic_search_test.config.ElasticsearchConnection;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;

import static elastic_search_test.config.DatasourceConstant.*;

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
    public void test1() {
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
}
