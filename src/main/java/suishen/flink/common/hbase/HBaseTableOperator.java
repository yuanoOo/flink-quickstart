package suishen.flink.common.hbase;

/**
 * @author zhaomingyuan
 * @date 18-10-11
 * @time 下午5:08
 */
public class HBaseTableOperator {
    private HBaseTablePut hBaseTablePut;
    private HBaseTableIncrement hBaseTableIncrement;

    public HBaseTablePut getHBaseTablePut() {
        return hBaseTablePut;
    }

    public HBaseTableOperator setHBaseTablePut(HBaseTablePut hBaseTablePut) {
        this.hBaseTablePut = hBaseTablePut;
        return this;
    }

    public HBaseTableIncrement getHBaseTableIncrement() {
        return hBaseTableIncrement;
    }

    public HBaseTableOperator setHBaseTableIncrement(HBaseTableIncrement hBaseTableIncrement) {
        this.hBaseTableIncrement = hBaseTableIncrement;
        return this;
    }
}
