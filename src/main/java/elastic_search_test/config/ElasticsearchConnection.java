package elastic_search_test.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Justin Wan justinxcwan@gmail.com
 */
public class ElasticsearchConnection {
    protected SimpleDateFormat indicesPattern;
    protected String indices;
    // Most likely to be daily or weekly
    protected long indexInterval;
    protected String[] types = {"*"};
    protected TransportClient client;

    public ElasticsearchConnection setIndices(String indices) {
        this.indices = indices;
        // clear any preset indices pattern
        this.indicesPattern = null;

        return this;
    }

    public ElasticsearchConnection setIndicesPattern(String pattern) {
        this.indices = null;

        // Try to be compatible with Kibana settings
        // [logstash-]YYYY.MM.DD.HH
        // [logstash-]YYYY.MM.DD
        // [logstash-]GGGG.WW
        // [logstash-]YYYY.MM
        // [logstash-]YYYY
        Pattern p = Pattern.compile("(.*)\\[(.*?)\\](.*)");
        Matcher m = p.matcher(pattern);
        m.matches();
        String datepattern = m.group(2).replace("YYYY", "yyyy")
                .replace("DD", "dd")
                .replace("WW", "ww")
                .replace("GGGG", "yyyy");

        pattern = m.group(1) + "'" + datepattern + "'" + m.group(3);

        this.indicesPattern = new SimpleDateFormat(pattern);

        if (pattern.indexOf("HH") >= 0) {
            indexInterval = 60 * 60 * 1000L; // one hour
        } else if (pattern.indexOf("dd") >= 0) {
            indexInterval = 24 * 60 * 60 * 1000L; // one hour
        } else if (pattern.indexOf("ww") >= 0) {
            indexInterval = 7 * 24 * 60 * 60 * 1000L; // one week
        } else if (pattern.indexOf("MM") >= 0) {
            // TODO()  possible bug?
            indexInterval = 30 * 7 * 24 * 60 * 60 * 1000L; // one month
        } else if (pattern.indexOf("yyyy") >= 0) {
            indexInterval = 365 * 24 * 60 * 60 * 1000L; // one year
        }
        return this;
    }

    public TransportClient client() {
        return client;
    }

    public String getTypes() {
        return Strings.arrayToCommaDelimitedString(types);
    }

    public ElasticsearchConnection setTypes(String... types) {
        this.types = types;

        return this;
    }

    public ElasticsearchConnection connect(String clusterName, String hosts, int port) throws UnknownHostException {
        final Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
        client = TransportClient.builder().settings(settings).build();

        String[] hostsList = hosts.split(",");

        for (String host : hostsList) {
//            int port = 9200;
//
//            // Can be written as localhost:9200
//            String[] hostWithPort = host.split(":");
//            if (hostWithPort.length > 1) {
//                port = Integer.parseInt(hostWithPort[1]);
//            }

            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        }

        return this;
    }

    /**
     * Find indices matching range [gte, lt).
     *
     * @param gte timestamp from
     * @param lt  timestamp until
     **/
    public String[] indicesForRange(long gte, long lt) {
        if (indices != null) {
            // No timestamp in indices
            return new String[]{indices};
        } else if (indicesPattern != null) {
            ArrayList<String> list = new ArrayList<>();

            // Use indexInterval/2 instead of indexInterval to avoid bugs when
            // intervals are month or year
            for (long ts = gte; ts <= lt; ts += indexInterval / 2) {
                String index = this.indicesPattern.format(new Date(ts));
                if (list.size() == 0 || !index.equals(list.get(list.size() - 1))) {
                    list.add(index);
                }
            }

            return list.toArray(new String[]{});
        } else {
            throw new IllegalArgumentException("indices and indicesPattern are all null");
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

}

