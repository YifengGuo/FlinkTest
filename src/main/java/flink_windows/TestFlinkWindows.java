package flink_windows;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * Created by guoyifeng on 9/11/18
 */
public class TestFlinkWindows {

    static DataStreamSource<String> input;
    static StreamExecutionEnvironment env;

    @Before
    public void initializeEnv() {
        // initialize Stream Execution Environment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create socket input stream
        input = env.socketTextStream("localhost", 9999);

    }


    @Test
    public void testReduceFunction() throws Exception {
        initializeEnv();
        DataStream<FlinkTestEvent> reducedStream = input
                .flatMap(new Splitter())
                .assignTimestampsAndWatermarks(new MyWatermarkAssigner())
                .keyBy((KeySelector<FlinkTestEvent, Object>) flinkTestEvent -> flinkTestEvent.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce((FlinkTestEvent e1, FlinkTestEvent e2) -> new FlinkTestEvent(e1.key, e1.value + e2.value, System.currentTimeMillis())
                );

        reducedStream.addSink(new SinkFunction<FlinkTestEvent>() {
            @Override
            public void invoke(FlinkTestEvent value, Context context) throws Exception {
                System.out.format("Key: %s   Value: %d   Timestamp %s \n", value.key, value.value, new Date(value.lastModifiedTime));
            }
        });
        // output of reducedStream
//    Key: a   Value: 4   Timestamp 15366527997188> flink_windows.FlinkTestEvent@74d1159d
//    Key: a   Value: 11   Timestamp 15366528031158> flink_windows.FlinkTestEvent@6298aedf
//    Key: b   Value: 8   Timestamp 15366527982733> flink_windows.FlinkTestEvent@2321aa54

        // reducedStream.print();
        env.execute();
    }

    /**
     * Test watermark periodically change will potentially change the size of tumbling window
     * not quite successful
     * @throws Exception
     */
    @Test
    public void testWatermarkShrinkWindowSize() throws Exception {
        initializeEnv();
        DataStream<Tuple2<String, FlinkTestEvent>> output = input
                .flatMap(new RichFlatMapFunction<String, FlinkTestEvent>() {
                    @Override
                    public void flatMap(String s, Collector<FlinkTestEvent> collector) throws Exception {
                        String[] tokens = s.split(" ");
                        FlinkTestEvent res = new FlinkTestEvent(tokens[0], Long.parseLong(tokens[1]), System.currentTimeMillis());
                        collector.collect(res);
                    }
                })
                .assignTimestampsAndWatermarks(new MyWatermarkAssigner())
                .keyBy(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .allowedLateness(Time.seconds(0))
                .apply(new MyWindowFunction());

        output.addSink(new SinkFunction<Tuple2<String, FlinkTestEvent>>() {
            @Override
            public void invoke(Tuple2<String, FlinkTestEvent> value, Context context) throws Exception {
                System.out.println(value.f0 + " " + value.f1);
            }
        });

        env.execute();

    }

    @Test
    public void testWatermark() throws Exception {
        initializeEnv();
        DataStreamSink<FlinkTestEvent> output = input
                .flatMap(new Splitter())
                .assignTimestampsAndWatermarks(new MyWatermarkAssigner())
                .addSink(new MySinkFunction());
        env.execute();
    }

    @Test
    public void testAggregateFunction() throws Exception {

    }

    class Splitter extends RichFlatMapFunction<String, FlinkTestEvent> {
        @Override
        public void flatMap(String s, Collector<FlinkTestEvent> collector) throws Exception {
            String[] tokens = s.split(" ");
            FlinkTestEvent res = new FlinkTestEvent(tokens[0], Long.parseLong(tokens[1]), System.currentTimeMillis());
            collector.collect(res);
        }
    }

    class MyWatermarkAssigner implements AssignerWithPunctuatedWatermarks<FlinkTestEvent> {
        private final long maxOutOfOrderness = 5000; // n seconds

        private long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(FlinkTestEvent lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

        @Override
        public long extractTimestamp(FlinkTestEvent element, long previousElementTimestamp) {
            currentMaxTimestamp = Math.max(element.lastModifiedTime, element.lastModifiedTime);
            return element.lastModifiedTime;
        }
    }

    class MyKeySelector implements KeySelector<FlinkTestEvent, Object> {
        @Override
        public String getKey(FlinkTestEvent flinkTestEvent) throws Exception {
            return flinkTestEvent.key;
        }
    }

//class MyProcessWindowFunction extends ProcessFunction<FlinkTestEvent, Tuple2<String, FlinkTestEvent>> {
//    @Override
//    public void processElement(FlinkTestEvent value, Context ctx, Collector<Tuple2<String, FlinkTestEvent>> out) throws Exception {
//        // directly collect all the elements in the window
//        out.collect(new Tuple2<>("result", value));
//    }
//}

    class MyWindowFunction implements WindowFunction<FlinkTestEvent, Tuple2<String, FlinkTestEvent>, Object, TimeWindow> {
        @Override
        public void apply(Object key, TimeWindow window, Iterable<FlinkTestEvent> input, Collector<Tuple2<String, FlinkTestEvent>> out) throws Exception {
            for (FlinkTestEvent event : input) {
                out.collect(new Tuple2<>(window.toString() + " " + window.maxTimestamp(), event));
            }
        }
    }

    class MySinkFunction extends RichSinkFunction<FlinkTestEvent> {
        @Override
        public void invoke(FlinkTestEvent value, Context context) throws Exception {
            System.out.println(context.currentWatermark() + " --> " + value);
        }

    }
}



