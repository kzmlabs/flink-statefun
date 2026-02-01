<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Release Notes: Flink 2.2.0 Migration

## Overview

This release migrates kzmlabs-flink-statefun from Apache Flink 1.16.2 to Apache Flink 2.2.0. This is a major upgrade that includes breaking API changes and requires Java 11+.

## Breaking Changes

### Source API Migration (Source V2)
- The deprecated `SourceFunction` and `RichSourceFunction` APIs have been replaced with the new `Source` interface
- All custom sources now implement `Source<T, SplitT, EnumChkT>` with separate `SourceReader`, `SourceSplit`, and `SplitEnumerator` components
- Affected components:
  - `SourceFunctionSpec` - now wraps `Source` instead of `SourceFunction`
  - `SupplyingSource` (harness module)
  - `CommandFlinkSource` (smoke e2e driver)

### Sink API Migration (Sink V2)
- The deprecated `SinkFunction` and `RichSinkFunction` APIs have been replaced with the new `Sink` interface
- All custom sinks now implement `Sink<T>` with a `SinkWriter` component
- Affected components:
  - `SinkFunctionSpec` - now wraps `Sink` instead of `SinkFunction`
  - `ConsumingSink` (harness module)
  - `SocketClientSink` (smoke e2e driver)
  - `DiscardingSink` import changed to `org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink`

### State Processor API Migration
- Migrated from batch `DataSet` API to streaming `DataStream` API
- `StatefulFunctionsSavepointCreator`:
  - Now accepts `DataStream` instead of `DataSet` for bootstrap data
  - Uses `SavepointWriter.newSavepoint()` instead of `Savepoint.create()`
  - Uses `OperatorIdentifier.forUid()` instead of string UID
  - Uses `StateBootstrapTransformation` instead of `BootstrapTransformation`
- `FunctionsStateBootstrapOperator`:
  - Uses `CheckpointingMode.EXACTLY_ONCE` instead of boolean flag
  - Uses `getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()` instead of `getIndexOfThisSubtask()`
- New `FunctionsStateBootstrapOperatorFactory` class added for operator creation
- `TaggedBootstrapDataTypeInfo.createSerializer()` now takes `SerializerConfig` instead of `ExecutionConfig`

### Configuration API Changes
- `ExecutionCheckpointingOptions` replaced with `CheckpointingOptions`
- `SavepointConfigOptions.SAVEPOINT_PATH` replaced with `StateRecoveryOptions.SAVEPOINT_PATH`
- `RocksDBStateBackend` replaced with `EmbeddedRocksDBStateBackend`
- `FsStateBackend` removed (use `HashMapStateBackend` or `EmbeddedRocksDBStateBackend`)

### Function API Changes
- `RichFlatMapFunction.open(Configuration)` signature changed to `open(OpenContext)`
- `RuntimeContext.getIndexOfThisSubtask()` moved to `RuntimeContext.getTaskInfo().getIndexOfThisSubtask()`

## Removed Features

### Kinesis Connector
- All Kinesis-related code has been removed
- The `flink-connector-kinesis` has been moved to a separate repository in Flink 2.x
- Affected packages removed:
  - `org.apache.flink.statefun.flink.io.kinesis.*`
  - `org.apache.flink.statefun.sdk.kinesis.*`
- If you need Kinesis support, you must add the separate Kinesis connector dependency

### DataSet API (flink-java)
- The `flink-java` dependency has been removed
- The DataSet API is deprecated in Flink 2.x
- All batch processing now uses the DataStream API with bounded sources

## New Dependencies

```xml
<!-- RocksDB State Backend (package changed) -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>2.2.0</version>
</dependency>
```

## Migration Guide

### Migrating Custom Sources

Before (Flink 1.x):
```java
public class MySource extends RichSourceFunction<T> {
    @Override
    public void run(SourceContext<T> ctx) { ... }

    @Override
    public void cancel() { ... }
}
```

After (Flink 2.x):
```java
public class MySource implements Source<T, MySplit, MyEnumCheckpoint> {
    @Override
    public Boundedness getBoundedness() { return Boundedness.CONTINUOUS_UNBOUNDED; }

    @Override
    public SourceReader<T, MySplit> createReader(SourceReaderContext ctx) { ... }

    @Override
    public SplitEnumerator<MySplit, MyEnumCheckpoint> createEnumerator(
        SplitEnumeratorContext<MySplit> ctx) { ... }

    // ... other required methods
}
```

### Migrating Custom Sinks

Before (Flink 1.x):
```java
public class MySink extends RichSinkFunction<T> {
    @Override
    public void invoke(T value, Context ctx) { ... }
}
```

After (Flink 2.x):
```java
public class MySink implements Sink<T> {
    @Override
    public SinkWriter<T> createWriter(WriterInitContext ctx) {
        return new MySinkWriter();
    }

    private class MySinkWriter implements SinkWriter<T> {
        @Override
        public void write(T element, Context ctx) { ... }

        @Override
        public void flush(boolean endOfInput) { ... }

        @Override
        public void close() { ... }
    }
}
```

### Migrating State Bootstrap Code

Before (Flink 1.x):
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
DataSet<MyData> data = env.fromElements(...);
creator.withBootstrapData(data, router);
```

After (Flink 2.x):
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<MyData> data = env.fromElements(...);
creator.withBootstrapData(data, router);
```

## Build Requirements

- **Java Version:** 21 (LTS) - required
- **Maven:** 3.6+

Note: This release requires Java 21. Earlier Java versions (8, 11, 17) are not supported.

## Compatibility

- This release is compatible with Apache Flink 2.2.0
- This release is NOT backward compatible with Flink 1.x

## Files Changed

### New Files
- `statefun-flink-harness/src/main/java/org/apache/flink/statefun/flink/harness/io/SupplyingSourceReader.java`
- `statefun-flink-harness/src/main/java/org/apache/flink/statefun/flink/harness/io/SupplyingSourceSplit.java`
- `statefun-flink-harness/src/main/java/org/apache/flink/statefun/flink/harness/io/SupplyingSourceSplitEnumerator.java`
- `statefun-flink-harness/src/main/java/org/apache/flink/statefun/flink/harness/io/SupplyingSourceSplitSerializer.java`
- `statefun-flink-state-processor/src/main/java/org/apache/flink/statefun/flink/state/processor/operator/FunctionsStateBootstrapOperatorFactory.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/CommandFlinkSourceReader.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/CommandFlinkSourceSplit.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/CommandFlinkSourceSplitEnumerator.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/CommandFlinkSourceSplitSerializer.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/CommandFlinkSourceSplitCheckpointSerializer.java`
- `statefun-smoke-e2e-driver/src/main/java/org/apache/flink/statefun/e2e/smoke/driver/SocketClientSink.java`

### Deleted Files
- All files under `statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kinesis/`
- All files under `statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/sdk/kinesis/`
- All files under `statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kinesis/`
- All files under `statefun-flink-io-bundle/src/test/resources/kinesis-io-binders/`

### Modified Files
- Root `pom.xml` - Updated Flink version to 2.2.0
- Multiple module `pom.xml` files - Dependency updates
- All Source/Sink related Java files - API migration
- All State Processor related Java files - API migration
- Configuration handling files - Updated to new config options

## Testing

All 34 modules build successfully and all tests pass.

```
[INFO] BUILD SUCCESS
[INFO] Total time: 01:18 min
[INFO] Reactor Summary:
[INFO] Kzmlabs StateFun Parent ............................ SUCCESS
[INFO] ... (all 34 modules) ...
```
