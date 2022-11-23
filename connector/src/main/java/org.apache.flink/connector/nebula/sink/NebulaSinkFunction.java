/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

public class NebulaSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final long serialVersionUID = 8100784397926666769L;

    private final NebulaBatchOutputFormat<T, ?> outputFormat;

    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public NebulaSinkFunction(NebulaBatchOutputFormat<T, ?> outputFormat) {
        super();
        this.outputFormat = outputFormat;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        try {
            outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        } catch (IOException e) {
            failureThrowable.compareAndSet(null, e);
        }
    }

    @Override
    public void close() {
        outputFormat.close();
    }

    @Override
    public void invoke(T value, Context context) {
        checkErrorAndRethrow();
        outputFormat.writeRecord(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        flush();
        checkErrorAndRethrow();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
        // nothing to do
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in NebulaSink.", cause);
        }
    }

    private void flush() throws IOException {
        outputFormat.flush();
    }
}
