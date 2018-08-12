## CheckPoint
https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/checkpoints.html

### 一、外化检查点（Externalized Checkpoints）

- 1、默认情况下，检查点不会在外部持久存在，仅用于从失败中恢复作业。取消程序时会删除它们。但是，
  您可以将定期检查点配置为在外部持久保存，与保存点类似。这些外部化检查点将 其元数据写入持久存储，
  并且在作业失败时不会自动清除。这样，如果您的工作失败，您将有一个检查点可以从中恢复。

  ```
  CheckpointConfig config = env.getCheckpointConfig();
  config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  ```

- 2、该ExternalizedCheckpointCleanup模式配置取消作业时外部化检查点发生的情况：
  - ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：取消作业时保留外部化检查点。请注意，在这种情况下，
    您必须在取消后手动清理检查点状态。
  - ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：取消作业时删除外部化检查点。只有在作业失败时，
    检查点状态才可用。

### 二、目录结构（Directory Structure）
- 1、与保存点类似，外部化检查点由元数据文件和一些其他数据文件组成，具体取决于状态后端。外部化检查点的元数据的
     目标目录由配置密钥state.checkpoints.dir确定，当前只能通过配置文件进行设置。
  ```
  state.checkpoints.dir: hdfs:///checkpoints/
  ```

- 2、然后，此目录将包含还原检查点所需的检查点元数据。对于MemoryStateBackend，此元数据文件将是自包含的，
     不需要其他文件。

- 3、FsStateBackend和RocksDBStateBackend编写单独的数据文件，只将这些文件的路径写入元数据文件。这些数据文件
    存储在构造期间给予状态后端的路径中。env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/");

### 三、与保存点的差异（Difference to Savepoints）
- 1、外部化检查点与保存点有一些差异。它们使用状态后端特定（低级）数据格式，可能是增量的，
     不支持Flink特定功能，如重新缩放。

### 四、从外部化检查点恢复（）
- 1、通过使用检查点的元数据文件，可以从外部检查点恢复作业，就像从保存点恢复一样（请参阅保存点恢复指南）。请注意，
     如果元数据文件不是自包含的，则作业管理器需要访问它所引用的数据文件（请参阅上面的目录结构）。
  ```
  $ bin/flink run -s :checkpointMetaDataPath [:runArgs]
  ```