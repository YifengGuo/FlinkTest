package flink_process_function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by guoyifeng on 8/23/18
 */
public class TestProcessFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStream<Tuple2<String, String>> sourceStream = env.fromElements(
//                Tuple2.of("fruit", "apple"),
//                Tuple2.of("fruit", "banana"),
//                Tuple2.of("meat", "beef"),
//                Tuple2.of("fruit", "peach"),
//                Tuple2.of("meat", "pork"),
//                Tuple2.of("meat", "fish"),
//                Tuple2.of("fruit", "avocado"),
//                Tuple2.of("fruit", "orange"),
//                Tuple2.of("meat", "steak")
//        );
//
//        env.setParallelism(1);
//
//        DataStream<Tuple2<String, Long>> result = sourceStream
//                .keyBy(0)
//                .process(new CountWithTimeoutFunction());
//
//
//        result.print();


        // read text from socket connection
        DataStream<Tuple2<String, Long>> res = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        res.print();

        env.execute("process function");
    }

    static class Splitter implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
            String[] key_value = s.split(" ");
            collector.collect(new Tuple2<>(key_value[0], key_value[1]));
        }
    }
}
