# Read file common core (0.0.5)

That is the core part for file reads written in Java or Kotlin. It provides the following functionality:

+ Monitoring the directory and new files in it;
+ Grouping files to different streams that will be read simultaneously
  (files in one stream will be read one by one according to they order - can be customized);
+ Ability to read files that are not completely written (e.g. log files);
+ Reporting errors on problems during decoding or with file content;
+ Fixing the timestamp that was extracted from the source in case it does not increase monotonically;
+ Adding sequences to the messages;

## Usage

### Create the reader

The base class for reader is `com.exactpro.th2.read.file.common.AbstractFileReader`.
If you want to implement the reader with your custom logic you need to extend this class.

However, there is a `com.exactpro.th2.read.file.common.impl.DefaultFileReader` class
that provides you the ability to configure it with lambda expressions.
If you don't need to store the state or use complex custom logic - this is your chose.

```kotlin
 val reader: AbstractFileReader<BufferedReader> = DefaultFileReader.Builder(
            configuration,
            directoryChecker,
            parser,
            movedFileTracker,
        ) { _, path -> BufferedReaderSourceWrapper(Files.newBufferedReader(path)) }
            .readFileImmediately()
            .onStreamData(onStreamData)
            .onError(onErrorAction)
            .acceptNewerFiles()
            .build()
```

**NOTE: the reader must be closed at the end of using.**

### Configuration

The abstract reader accepts the `com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration`.
It contains the following parameters:

```kotlin
class CommonFileReaderConfiguration(
    /**
     * The timeout since the last file modification
     * before the reader starts to consider the source as finished (it won't be changes any more).
     * It allows the [com.exactpro.th2.read.file.common.ContentParser] to parse the data at the end of the file
     * because it won't change anymore.
     *
     * NOTE: if the file is actually changed after that,
     * the file is the last one for the [com.exactpro.th2.read.file.common.StreamId]
     * and [leaveLastFileOpen] is enabled the new data will be read
     */
    val staleTimeout: Duration = Duration.ofSeconds(5),

    /**
     * The maximum number of messages in a one batch.
     * If the reader tries to add a new portion of messages
     * to the batch and the new size is bigger than the [maxBatchSize]
     * the previous messages will be published before the new messages is added to the batch
     */
    val maxBatchSize: Int = 100,

    /**
     * The max delay the reader can delay the publication and accumulate the batch.
     *
     * NOTE: the reader published the batches only during update processing.
     * If the update processing method is not invoked the delayed batches won't be published
     * util the method is invoked. The actual delay might be bigger.
     */
    val maxPublicationDelay: Duration = Duration.ofSeconds(1),

    /**
     * Do not close the last file for stream ID until the new one is not found or the reader is not stopped
     */
    val leaveLastFileOpen: Boolean = true,
    /**
     * If it is enabled the incorrect timestamp (less than the previous one for the [com.exactpro.th2.read.file.common.StreamId])
     * will be fixed. Otherwise, the exception will be thrown and the source processing will be stopped
     */
    val fixTimestamp: Boolean = false,

    /**
     * Defines how many batches might be published in one second for a [com.exactpro.th2.read.file.common.StreamId].
     * If limit is reached the source reading will be suspended until the next second
     */
    val maxBatchesPerSecond: Int = UNLIMITED_PUBLICATION,
)
```

#### Configuration deserialization

The configuration uses the `Duration` class for defining the timeout. The default Jackson deserializer does not support it.
You need to use [JavaTime modules](https://github.com/FasterXML/jackson-modules-java8).

The module you need is `com.fasterxml.jackson.datatype:jackson-datatype-jsr310`.

## Changes

### 0.0.5

+ Associate one file with multiple stream IDs;