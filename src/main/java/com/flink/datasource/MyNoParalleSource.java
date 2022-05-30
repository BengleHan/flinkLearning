package com.flink.datasource;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
public class MyNoParalleSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {

        while (isRunning) {
            sourceContext.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);

        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}
