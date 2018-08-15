package cn.jxau.yuan.scala.yuan.java.async;
// This example implements the asynchronous request and callback with Futures that have the
// interface of Java 8's futures (which is the same one followed by Rescaling Stateful Applications.md's Future)

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * 官网中的例子,更接近实际的情况.
 * 当没有异步客户端的时候,可以使用线程池,适当的提高性能.见flink source code中的example
 *
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    /** The database specific client that can issue concurrent requests with callbacks */
    private transient DatabaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new DatabaseClient( /**host, post, credentials **/);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

        // issue the asynchronous request, receive a future for result
        final Future<String> result = client.query(key);

        // set the callback to be executed once the request by the client is complete the callback simply forwards the result to the result future
        // 设置一旦客户端的请求完成后执行回调，回调只是将结果转发给result future
        CompletableFuture.supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                // Normally handled explicitly.
                return null;
            }
        }).thenAccept(dbResult -> resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult))));
    }
}

// create the original stream
//DataStream<String> stream = ...;

// apply the async I/O transformation
//DataStream<Tuple2<String, String>> resultStream =
//    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);

class DatabaseClient {
    public void close() {

    }

    public Future<String> query(String key) {
        return null;
    }
}