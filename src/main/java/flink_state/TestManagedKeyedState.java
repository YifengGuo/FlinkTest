package flink_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by guoyifeng on 8/19/18
 */
public class TestManagedKeyedState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 4L), Tuple2.of(1L, 5L), Tuple2.of(1L, 3L), Tuple2.of(1L, 8L), Tuple2.of(1L, 6L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute();
    }
}

class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    public transient ValueState<Tuple2<Long, Long>> sum; // state stores sum of input's value

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> collector) throws Exception {
        // access the state
        Tuple2<Long, Long> currentSum = sum.value();

        // update currentSum count and sum by current input
        currentSum.f0 += 1;
        currentSum.f1 += input.f1;
        // update sum by currentSum
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state (usually we will input KeyedStream so
        // that input.f0 is all the same
        if (currentSum.f0 >= 2) {
            collector.collect(new Tuple2<Long, Long>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) throws Exception {
        // get the handle of State
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set

        sum = getRuntimeContext().getState(descriptor); // access state by RuntimeContext
    }
}
