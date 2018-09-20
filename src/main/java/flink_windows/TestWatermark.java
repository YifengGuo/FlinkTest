package flink_windows;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangwei1025 on 9/12/18.
 */
public class TestWatermark {

    private List<String> source = new ArrayList<>();

    @Before
    public void setUp() {
        long current=System.currentTimeMillis()/1000*1000;
        for (int i = 1; i <= 10; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", i);
            jsonObject.put("occur_time", current+100L * i);
            source.add(jsonObject.toJSONString());
        }
    }

    @Test
    public void windowAllWatermark() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.disableOperatorChaining();
        DataStream<JSONObject> sourceStream = env.fromCollection(source)
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        System.out.println("source:"+value);
                        out.collect(jsonObject);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<JSONObject>() {
                    long currentTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(JSONObject lastElement, long extractedTimestamp) {
                        return new Watermark(extractedTimestamp);
                    }

                    @Override
                    public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
                        currentTimestamp = element.getLong("occur_time");
                        return element.getLong("occur_time");
                    }
                });

        DataStream<JSONObject> window1Stream = sourceStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                System.out.println("keybytest1 receive:"+value.toJSONString());
                return "id";
            }
        }).window(TumblingEventTimeWindows.of(Time.milliseconds(500L)))
                .apply(new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                        System.out.println("window1 start:"+window.getStart());
                        for (JSONObject jsonObject : input) {
                            System.out.println(jsonObject);
                            jsonObject.put("flag","afterWindow1");
                        }
                        System.out.println("window1 end:"+window.getEnd());
                        for (JSONObject jsonObject : input) {
                            out.collect(jsonObject);
                        }
                    }
                });

        DataStream<JSONObject> window2Stream = window1Stream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                System.out.println("Map receive:"+value);
                return value;
            }
        })
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<JSONObject>() {
                    long currentTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(JSONObject lastElement, long extractedTimestamp) {
                        return new Watermark(extractedTimestamp);
                    }

                    @Override
                    public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
                        currentTimestamp = element.getLong("occur_time");
                        return element.getLong("occur_time");
                    }
                })
                .keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                System.out.println("keyby2 receive:"+value.toJSONString());
                return "id";
            }
        }).window(TumblingEventTimeWindows.of(Time.milliseconds(500L),Time.milliseconds(150L)))
                .apply(new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                        System.out.println("window2 start:"+window.getStart());
                        for (JSONObject jsonObject : input) {
                            System.out.println(jsonObject);
                        }
                        System.out.println("window2 end:"+window.getEnd());
                        for (JSONObject jsonObject : input) {
                            out.collect(jsonObject);
                        }
                    }
                });

        env.execute("testWatermark");
    }
}

