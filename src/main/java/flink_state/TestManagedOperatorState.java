package flink_state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by guoyifeng on 8/19/18
 */
public class TestManagedOperatorState {
    public static void main(String[] args) throws Exception {
        String path = "src/main/java/flink_state/sink_output/";
        Tuple2<String, Integer> t1 = new Tuple2<>("id", 12345);
        Tuple2<String, Integer> t2 = new Tuple2<>("salary", 1000);
        Tuple2<String, Integer> t3 = new Tuple2<>("phone_num", 732888888);

        Tuple2<String, Integer>[] arr = (Tuple2<String, Integer>[]) new Tuple2[]{t1, t2, t3};
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.fromCollection(Arrays.asList(arr))
//                .addSink(new BufferingSinkFunction(2));
        env.fromElements(Tuple2.of("id", 12345), Tuple2.of("salary", 1000), Tuple2.of("phone_num", 732888888))
                .keyBy(0)
                .addSink(new BufferingSinkFunction(1, path));
        env.execute("buffer elements");
    }
}

/**
 *  uses CheckpointedFunction to buffer elements before sending them to the outside world.
 *  It demonstrates the basic even-split redistribution list state
 */
class BufferingSinkFunction implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private volatile List<Tuple2<String, Integer>> bufferedElements;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private final int threshold;

     private final String path;

    // constructor
    public BufferingSinkFunction(int threshold, String path) {
        this.threshold = threshold;
        bufferedElements = new ArrayList<>();
        this.path = path;
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value); // buffer elements before sinking them outside
        if (bufferedElements.size() == threshold) {
            File file = new File(path + "" + value.f0 + ".txt");
            FileWriter fw = new FileWriter(file);
            for (Tuple2<String, Integer> tuple : bufferedElements) {
                // sink value to the outside
                fw.write(tuple.toString() + "\n");
            }
            fw.flush();
            fw.close();
            bufferedElements.clear();
        }
    }

    /**
     * do the snapshot on the current state: copy all the elements from bufferedElements to state
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear(); // remove previous state info
        for (Tuple2<String, Integer> value : bufferedElements) {
            checkpointedState.add(value);
        }
    }

    /**
     * initializeState() will be invoked both when the function is firstly initialized or
     *                   when recovering from previous checkpoint
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // get a handle of state
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );

        // initialize state
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        // recovery case from earlier state
        if (context.isRestored()) {
            for (Tuple2<String, Integer> historicalValue : checkpointedState.get()) {
                bufferedElements.add(historicalValue);
            }
        }
    }
}


