package flink_metrics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by guoyifeng on 9/19/18
 */
public class TestFlinkCounter {
    static DataStreamSource<String> input;
    static StreamExecutionEnvironment env;

    public final Logger LOG = LoggerFactory.getLogger(TestFlinkCounter.class);

    @Before
    public void initializeEnv() {
        // initialize Stream Execution Environment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create socket input stream
        input = env.socketTextStream("localhost", 9999);
    }

    @Test
    public void testCounter() throws Exception {
        initializeEnv();
        DataStream<Tuple2<String, Integer>> output = input
//                .setParallelism(1)
                .map(new MyMapper())
                .keyBy(0)
                .reduce(new MyReducer());

        env.execute();
        output.print();

//        Map<String, Object> accumulatorMap = env.execute().getAllAccumulatorResults();

    }
}

class MyMapper extends RichMapFunction<String, Tuple2<String, Integer>> {

    private static volatile Counter counter;  // need to set as static volatile unless the counter would not change
    public final Logger LOG = LoggerFactory.getLogger(MyMapper.class);

    @Override
    public void open(Configuration config) throws Exception {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
    }

    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        String[] tokens = s.split(" ");
        Tuple2<String, Integer> out = new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        this.counter.inc();
//        LOG.info("current input count is " + this.counter.getCount());
        return out;
    }
}

class MyReducer extends RichReduceFunction<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
    }
}
