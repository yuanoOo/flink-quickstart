package cn.jxau.yuan.scala.yuan.java.async.kudu;

import com.stumbleupon.async.Callback;
import kudu.internal.KuduTuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.Collections;

/**
 * 1、现在的场景，KuduTable是固定不变的，没有必要每次都去获取，在open中初始化即可
 *
 * @author zhaomingyuan
 * @date 18-8-22
 * @time 下午2:33
 */
public class KuduAsyncFunction extends RichAsyncFunction<KuduTuple, Void>{

    private static final String KUDU_MASTER = "node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051";
    private static final String KUDU_TABLE = "ods_kudu_pv_event_1d";

    private transient AsyncKuduClient asyncKuduClient;
    private transient KuduTable table;
    private transient AsyncKuduSession asyncKuduSession;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        asyncKuduClient = new AsyncKuduClient.AsyncKuduClientBuilder(KUDU_MASTER).build();
        table = asyncKuduClient.openTable(KUDU_TABLE).join(50000000);
        asyncKuduSession = asyncKuduClient.newSession();
    }


    @Override
    public void asyncInvoke(KuduTuple tuple, ResultFuture<Void> resultFuture) throws Exception {
        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();

        for (int i = 0; i < table.getSchema().getColumnCount(); i++) {
            ColumnSchema columnSchema = table.getSchema().getColumnByIndex(i);
            Type type = columnSchema.getType();
            switch (type) {
                case BINARY:
                    row.addBinary(i, (byte[]) tuple.getField(i));
                    break;
                case STRING:
                    row.addString(i, tuple.getField(i));
                    break;
                case BOOL:
                    row.addBoolean(i, tuple.getField(i));
                    break;
                case DOUBLE:
                    row.addDouble(i, tuple.getField(i));
                    break;
                case FLOAT:
                    row.addFloat(i, tuple.getField(i));
                    break;
                case INT8:
                    row.addByte(i, tuple.getField(i));
                    break;
                case INT16:
                    row.addShort(i, tuple.getField(i));
                    break;
                case INT32:
                    row.addInt(i, tuple.getField(i));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    row.addLong(i, tuple.getField(i));
                    break;
                default:
                    throw new IllegalArgumentException("Illegal var type: " + type);
            }
        }
        asyncKuduSession.apply(upsert).addCallback(new Callback<Void, OperationResponse>() {
            @Override
            public Void call(OperationResponse operationResponse) throws Exception {
                   if (operationResponse.hasRowError()){
                       resultFuture.completeExceptionally(new Exception(operationResponse.getRowError().toString()));
                   }

                   resultFuture.complete(Collections.emptyList());

                   return null;
            }
        });
    }


    @Override
    public void close() throws Exception {
        super.close();
        asyncKuduSession.close();
        asyncKuduClient.close();
    }
}
