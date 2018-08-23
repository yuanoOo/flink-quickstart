package cn.jxau.yuan.scala.yuan.java.async.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.*;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhaomingyuan
 * @date 18-8-23
 * @time 上午10:47
 */
public class AsyncAerospikeFunction extends RichAsyncFunction<String, String>{

    private static final String AEROSPIKE_HOST = "localhost";

    private AerospikeClient client;
    private EventLoops eventLoops;
    private Monitor monitor;
    private AtomicInteger recordCount;
    private final int recordMax = 100000;
    // Records TTL time
    private final int writeTimeout = 5000;
    private int eventLoopSize;
    // Maximum number of connections allowed per server node.
    private int concurrentMax;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Allocate an event loop for each cpu core.
        eventLoopSize = Runtime.getRuntime().availableProcessors();
        // Allow 40 concurrent commands per event loop.
        concurrentMax = eventLoopSize * 40;

        monitor = new Monitor();
        recordCount = new AtomicInteger();

        //----------------Asynchronous event loop configuration.-------------//
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.minTimeout = writeTimeout;


        //----------------Select NIO Class-------------//
        // Direct NIO
        eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);

        // Netty NIO
        // EventLoopGroup group = new NioEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        // Netty epoll (Linux only)
        // EventLoopGroup group = new EpollEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);


        //----------------Container object for client policy Command.-----------//
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        clientPolicy.maxConnsPerNode = concurrentMax;
        clientPolicy.writePolicyDefault.setTimeout(writeTimeout);

        client = new AerospikeClient(clientPolicy, AEROSPIKE_HOST, 3000);
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        Key key = new Key("test", "test", UUID.randomUUID().toString());
        Bin bin = new Bin("bin", UUID.randomUUID().toString());
        client.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                resultFuture.complete(Collections.emptyList());
            }

            @Override
            public void onFailure(AerospikeException exception) {
                resultFuture.completeExceptionally(new Exception());
            }
        }, null, key, bin);
    }

    @Override
    public void close() throws Exception {
        super.close();
        eventLoops.close();
        client.close();
    }

    private void writeRecords() {
        // Write exactly concurrentMax commands to seed event loops.
        // Distribute seed commands across event loops.
        // A new command will be initiated after each command completion in WriteListener.
        for (int i = 1; i <= concurrentMax; i++) {
            EventLoop eventLoop = eventLoops.next();
            writeRecord(eventLoop, new AWriteListener(eventLoop), i);
        }
    }

    private void writeRecord(EventLoop eventLoop, WriteListener listener, int keyIndex) {
        Key key = new Key("test", "test", keyIndex);
        Bin bin = new Bin("bin", keyIndex);
        client.put(eventLoop, listener, null, key, bin);
    }

    private class AWriteListener implements WriteListener {

        private final EventLoop eventLoop;

        public AWriteListener(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        @Override
        public void onSuccess(Key key) {
            try {
                int count = recordCount.incrementAndGet();

                // Stop if all records have been written.
                if (count >= recordMax) {
                    monitor.notifyComplete();
                    return;
                }

                if (count % 10000 == 0) {
                    System.out.println("Records written: " + count);
                }

                // Issue one new command if necessary.
                int keyIndex = concurrentMax + count;
                if (keyIndex <= recordMax) {
                    // Write next record on same event loop.
                    writeRecord(eventLoop, this, keyIndex);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                monitor.notifyComplete();
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            e.printStackTrace();
            monitor.notifyComplete();
        }
    }
}
