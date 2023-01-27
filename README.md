# Read file common core for common (1.5.1)

That is the core part for file reads written in Java or Kotlin. It provides the following functionality:

+ Monitoring the directory and new files in it;
+ Grouping files to different streams that will be read simultaneously
  (files in one stream will be read one by one according to they order - can be customized);
+ Ability to read files that are not completely written (e.g. log files);
+ Reporting errors on problems during decoding or with file content;
+ Fixing the timestamp that was extracted from the source in case it does not increase monotonically;
+ Adding sequences to the messages;

## Usage

### Files requirements

If you are going to use the `reader` to read data from the file that receives new data at the same time the data should be only appended to that file.
NOTE: **the data should be appended to exactly the same file**.

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
    /**
     * Disables tracking of files' movements.
     * The reader won't be able to determinate whether the already read file was moved or deleted and a new file with the same name was added a bit later.
     * But the reading will be a lot faster because the reader does not need to keep tracking updates from file system
     */
    val disableFileMovementTracking: Boolean = false,

    /**
     * If this setting is set to `true` the reader will reopen the file if it detects that it was truncated (the size is less than the original one).
     * If this setting is set to `false` the error will be reported for the StreamId and not more data will be read.
     */
    val allowFileTruncate: Boolean = false,

    /**
     * If enabled the reader will continue processing files for **StreamID** if an error was occurred when processing files for that stream.
     * The file that caused an error will be skipped and marked as processed.
     *
     * If disabled the reader will stop processing files for **StreamID** if any error was occurred
     */
    val continueOnFailure: Boolean = false,

    /**
     * The min amount of time that must pass before the read will pull updates from the files system if it constantly read data.
     * This parameter is ignored if:
     * + reading from one of the streams has been finished
     */
    val minDelayBetweenUpdates: Duration = Duration.ZERO
)
```

#### Configuration deserialization

The configuration uses the `Duration` class for defining the timeout. The default Jackson deserializer does not support it.
You need to use [JavaTime modules](https://github.com/FasterXML/jackson-modules-java8).

The module you need is `com.fasterxml.jackson.datatype:jackson-datatype-jsr310`.

#### Behavior details

Each file will produce group of messages that can include `th2.read.order_marker` property:
 * First and last messages will be marked as `start` and `fin`. If first message wasn't sent, `start` mark will be moved to next one 
until some messages will be actually sent.
 * If file produced only one message, it will be marked as `single` 

 **NOTE**: If option `leaveLastFileOpen` is turned on means no flags will be generated because in this case the last message read from the file cannot be determinate

#### Metrics

The common-read-core exports the following metrics:

+ Files:
  + **th2_read_files_in_dir_current** - the current number of files in directory that can be processed or was processed by the reader
  + **th2_read_processed_files_count** - the number of files that was handled by the reader with their state:
    + found - reader found the files and start processing it
    + processed - reader have processed the file and closed it
    + error - an error was occurred during file processing
    + dropped - the stream ID corresponding to the file was added to ignore list (because of previous error) and file was not processed
+ Processing time:
  + **th2_read_processing_time** - the time that was spent by the reader to perform a certain action:
    + pull - gathering files to process
    + read - reading a message from the file

#### Build-in filters

The common core part for read contains build-in filters to filter up the content.
It filters messages and files based on the current state and file information

##### OldTimestampMessageFilter

This filters drops messages and files that contains old data.

The message will be dropped if the current state for StreamId contains **lastTimestamp** greater or equals to the timestamp from message.

The file will be dropped if its last modification time is less than **lastTimestamp** for current StreamID + **staleTimeout**.

## Changes

### 1.5.1

+ BOM updated from `4.0.2` to `4.1.0`
+ kotlin-logging updated to `3.0.2`


### 1.5.0

+ Added `continueOnFailure` option to continue processing files for **StreamId** if an error is occurred during file processing for that **StreamId**
+ Added `minDelayBetweenUpdates` option to specify the min delay between pulling updates from the file system.
  Can improve the performance. However, it will take longer to handle a new file.
  It will be found only if one of the current sources is finished or the time since the last updates check exceeds the `minDelayBetweenUpdates`.
+ Allow zero stale timeout
+ Added metrics for reader performance
+ Caching of directory updates during each processing iteration
+ Dependencies with vulnerabilities was updated:
    + Kotlin updated from `1.4.32` to `1.6.21`
    + BOM updated from `3.0.0` to `4.0.2`
    + grpc-common updated from `3.1.2` to `3.11.1`
    + log4j 1.2 removed from dependencies

### 1.4.0

+ Added a new interface `EndAwareFileSourceWrapper` that can indicate whether the source does not have any additional data and no data will be added in future

### 1.3.0

+ Adds new property `th2.read.order_marker` for first and last messages [`start`/`fin`] or [`single`] for files with single message

### 1.2.2

+ Change the way the `DirectoryChecker` gets the list of files in the directory. Check file existence before filtering it.

### 1.2.1

+ Fix problem with incorrect sequences when the 'onContentRead' returns empty collection

### 1.2.0

+ Add handling truncated files. Option `allowFileTruncate` for allowing that is added to the configuration

### 1.1.0

+ Adds the ability to recover the data source (reopen and restore its state according to the current source state)

### 1.0.0

+ Split versions for common V2 and common V3 (1.*.* - common V3, 0.* - common V2)

### 0.0.7

+ Add parameter for disabling file movement tracking

### 0.0.5

+ Associate one file with multiple stream IDs;