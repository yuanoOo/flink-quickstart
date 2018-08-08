 ## Myself QA List

- 1,下面关于checkpoint的参数,有待验证了解,应该是一个比较重要的参数
```
    /**
     * 设置检查点尝试之间的最小暂停。
     * 此设置定义检查点协调器在可以触发与最大并发检查点数相关的另一个检查点之后可以多快触发另一个检查点
     * （请参阅{@link #setMaxConcurrentCheckpoints（int）}）。
     * 
     * 如果将最大并发检查点数设置为1，则此设置可以有效地确保在没有检查点进行的情况下经过最短时间。
     * 
     * the maximum number of concurrent checkpoints
     * 最大并发检查点数:暂时还不理解这个东西的含义,有待了解
     */
    checkpointConf.setMinPauseBetweenCheckpoints(30000L);
```