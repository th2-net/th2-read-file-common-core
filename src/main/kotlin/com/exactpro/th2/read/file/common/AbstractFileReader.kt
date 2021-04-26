/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.toInstant
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.StreamData
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.math.abs

abstract class AbstractFileReader<T : AutoCloseable>(
    private val configuration: CommonFileReaderConfiguration,
    private val directoryChecker: DirectoryChecker,
    private val contentParser: ContentParser<T>,
    private val readerState: ReaderState,
    private val onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit,
    private val onError: (StreamId?, String, Exception) -> Unit = { _, _, _ -> },
    private val sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR
) : AutoCloseable {
    @Volatile
    private var closed: Boolean = false

    private val currentFilesByStreamId: MutableMap<StreamId, FileHolder<T>> = ConcurrentHashMap()
    private val contentByStreamId: MutableMap<StreamId, PublicationHolder> = ConcurrentHashMap()

    private lateinit var fileTracker: MovedFileTracker
    private val trackerListener = object : MovedFileTracker.FileTrackerListener {
        override fun moved(prev: Path, current: Path) {
            val holderWithPathMatch = currentFilesByStreamId.values.find { it.path == prev }
            if (holderWithPathMatch != null) {
                LOGGER.info { "File $prev moved to $current" }
                holderWithPathMatch.moved()
                readerState.fileProcessed(current)
            } else {
                if (readerState.processedFileRemoved(prev)) {
                    LOGGER.info { "Already processed $prev file was moved to $current. Update processed files list" }
                    readerState.fileProcessed(current)
                }
            }
        }

        override fun removed(paths: Set<Path>) {
            readerState.processedFilesRemoved(paths)
            val holderWithRemovedFiles = currentFilesByStreamId.values
                .filter { paths.contains(it.path) }
            LOGGER.info { "Files removed: ${holderWithRemovedFiles.joinToString(", ") { it.path.toString() }}" }
            holderWithRemovedFiles.forEach { it.removed() }
        }
    }

    private class PublicationHolder {
        private var _creationTime: Instant = Instant.now()
        val creationTime: Instant
            get() = _creationTime

        /**
         * Do not forget to copy the content before passing it to anywhere
         */
        val content: MutableList<RawMessage.Builder> = arrayListOf()
        fun reset() {
            _creationTime = Instant.now()
            content.clear()
        }
    }

    fun init(fileTracker: MovedFileTracker) {
        this.fileTracker = fileTracker
        fileTracker += trackerListener
    }

    fun processUpdates() {
        check(!closed) { "Reader already closed" }
        check(::fileTracker.isInitialized) { "The reader must be init first" }

        LOGGER.debug { "Checking updates" }
        try {
            do {
                val holdersByStreamId: Map<StreamId, FileHolder<T>> = holdersToProcess()
                LOGGER.trace { "Get ${holdersByStreamId.size} holder(s) to process" }
                for ((streamId, fileHolder) in holdersByStreamId) {
                    LOGGER.trace { "Processing holder for $streamId. $fileHolder" }
                    val sourceWrapper: FileSourceWrapper<T> = fileHolder.sourceWrapper
                    val readContent: Collection<RawMessage.Builder> = try {
                        readMessages(streamId, fileHolder)
                    } catch (ex: Exception) {
                        LOGGER.error(ex) { "Error during reading messages for $streamId. File holder: $fileHolder" }
                        onError(streamId, "Cannot read data from the file ${fileHolder.path}", ex)
                        failStreamId(streamId, fileHolder, ex)
                        continue
                    }

                    if (readContent.isEmpty()) {
                        if (!sourceWrapper.hasMoreData) {
                            closeSourceIfAllowed(streamId, fileHolder)
                        }
                        continue
                    }

                    val finalContent = onContentRead(streamId, fileHolder.path, readContent)

                    finalContent.also { content ->
                        val streamData = readerState[streamId]
                        setCommonInformation(streamId, content, streamData)
                        try {
                            validateContent(streamId, content, streamData)
                        } catch (ex: Exception) {
                            LOGGER.error(ex) { "Failed to validate content for stream $streamId ($fileHolder):" +
                                " ${content.joinToString { shortDebugString(it) }}" }
                            failStreamId(streamId, fileHolder, ex)
                            return@also
                        }
                        tryPublishContent(streamId, content)
                    }
                }

                for ((streamId, holder) in contentByStreamId) {
                    holder.tryToPublish(streamId)
                }
            } while (holdersByStreamId.isNotEmpty() && !Thread.currentThread().isInterrupted)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Error during processing updates" }
            onError(null, "Error during processing updates", ex)
            if (ex is InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    override fun close() {
        if (closed) {
            LOGGER.warn { "Reader already closed" }
            return
        }
        try {
            contentByStreamId.forEach { (streamId, holder) ->
                with(holder) {
                    if (content.isNotEmpty()) {
                        publish(streamId)
                    }
                }
            }
            currentFilesByStreamId.values.forEach(this::closeSource)
            if (::fileTracker.isInitialized) {
                fileTracker -= trackerListener
            }
        } finally {
            closed = true
        }
    }

    protected open fun canBeClosed(streamId: StreamId, fileHolder: FileHolder<T>): Boolean {
        val canCloseTheLastFile = canCloseTheLastFileFor(streamId)
        return (canCloseTheLastFile && noChangesForStaleTimeout(fileHolder)) || !fileHolder.isActual || !fileHolder.stillExist
    }

    /**
     * Will be invoked when the new source file for [streamId] is found.
     */
    protected open fun onSourceFound(
        streamId: StreamId,
        path: Path,
    ) {
        // do nothing
    }

    /**
     * Will be invoke on each read content.
     * Can be used to modify the [RawMessage.Builder] before publishing them
     */
    protected open fun onContentRead(
        streamId: StreamId,
        path: Path,
        readContent: Collection<RawMessage.Builder>,
    ): Collection<RawMessage.Builder> {
        return readContent
    }

    /**
     * Will be invoked when an error is accurate during processing the source from [path] file for [StreamId]
     */
    protected open fun onSourceCorrupted(
        streamId: StreamId,
        path: Path,
        cause: Exception,
    ) {
        // do nothing
    }

    /**
     * Will be invoked when the source if finished and is closed
     */
    protected open fun onSourceClosed(
        streamId: StreamId,
        path: Path,
    ) {
        // do nothing
    }

    protected abstract fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean

    protected abstract fun acceptFile(streamId: StreamId, currentFile: Path?, newFile: Path): Boolean

    protected abstract fun createSource(streamId: StreamId, path: Path): FileSourceWrapper<T>

    private fun setCommonInformation(
        streamId: StreamId,
        readContent: Collection<RawMessage.Builder>,
        streamData: StreamData?
    ) {
        var sequence: Long = streamData?.run { lastSequence + 1 } ?: sequenceGenerator(streamId)
        readContent.forEach {
            it.metadataBuilder.apply {

                if (!hasTimestamp()) {
                    timestamp = Instant.now().toTimestamp()
                }

                idBuilder.apply {
                    connectionIdBuilder.sessionAlias = streamId.sessionAlias
                    direction = streamId.direction
                    setSequence(sequence++)
                }
            }
        }
    }

    private fun noChangesForStaleTimeout(fileHolder: FileHolder<T>): Boolean = !fileHolder.changed &&
        abs(System.currentTimeMillis() - fileHolder.lastModificationTime.toMillis()) > configuration.staleTimeout.toMillis()

    private fun canCloseTheLastFileFor(streamId: StreamId): Boolean {
        return if (configuration.leaveLastFileOpen) {
            hasNewFilesFor(streamId)
        } else {
            true
        }
    }

    private fun hasNewFilesFor(streamId: StreamId): Boolean = pullUpdates().containsKey(streamId)

    private fun tryPublishContent(
        streamId: StreamId,
        readContent: Collection<RawMessage.Builder>
    ) {
        val publicationHolder = contentByStreamId.computeIfAbsent(streamId) { PublicationHolder() }
        with(publicationHolder) {
            tryToPublish(streamId, readContent.size)
            content.addAll(readContent)
        }
    }

    private fun PublicationHolder.tryToPublish(
        streamId: StreamId,
        addToBatch: Int = 0
    ) {
        if (content.isEmpty()) {
            reset()
            return
        }
        val newSize = content.size + addToBatch
        if (newSize >= configuration.maxBatchSize || timeForPublication(creationTime)) {
            LOGGER.debug { "Publish the content for stream $streamId. Content size: ${content.size}; Creation time: $creationTime" }
            publish(streamId)
        }
    }

    private fun PublicationHolder.publish(streamId: StreamId) {
        onStreamData(streamId, content.toList())
        reset()
    }

    private fun timeForPublication(creationTime: Instant): Boolean {
        return Duration.between(creationTime, Instant.now()).abs() > configuration.maxPublicationDelay
    }

    private fun closeSourceIfAllowed(
        streamId: StreamId,
        fileHolder: FileHolder<T>,
    ) {
        val path = fileHolder.path
        LOGGER.debug { "Source for $path file does not have any additional data yet. Check if we can close it" }
        if (canBeClosed(streamId, fileHolder)) {
            terminateSource(streamId, fileHolder)
            onSourceClosed(streamId, fileHolder.path)
        }
    }

    private fun failStreamId(
        streamId: StreamId,
        fileHolder: FileHolder<T>,
        cause: Exception
    ) {
        LOGGER.debug { "Terminating source from file ${fileHolder.path} for stream $streamId" }
        terminateSource(streamId, fileHolder)
        readerState.excludeStreamId(streamId)
        onSourceCorrupted(streamId, fileHolder.path, cause)
    }

    private fun terminateSource(
        streamId: StreamId,
        fileHolder: FileHolder<T>
    ) {
        closeSource(fileHolder)
        currentFilesByStreamId.remove(streamId)
        if (fileHolder.isActual) {
            readerState.fileProcessed(fileHolder.path)
        }
    }

    private fun closeSource(fileHolder: FileHolder<T>) {
        val path = fileHolder.path
        LOGGER.info { "Closing source for $path file" }
        runCatching { fileHolder.close() }
            .onSuccess { LOGGER.debug { "Source for file $path successfully closed" } }
            .onFailure { LOGGER.error(it) { "Cannot close source for file $path" } }
    }

    private fun readMessages(
        streamId: StreamId,
        holder: FileHolder<T>
    ): Collection<RawMessage.Builder> {
        if (!holder.sourceWrapper.hasMoreData) {
            return emptyList()
        }
        var content: Collection<RawMessage.Builder> = emptyList()

        with(holder.sourceWrapper) {
            do {
                mark()
                val canParse: Boolean = contentParser.canParse(streamId, source, noChangesForStaleTimeout(holder))
                reset()
                if (canParse) {
                    content = contentParser.parse(streamId, source)
                    if (content.isNotEmpty()) {
                        LOGGER.trace { "Read ${content.size} message(s) for $streamId from ${holder.path}" }
                        break
                    }
                }
            } while (canParse && hasMoreData)
        }
        return content
    }

    private fun validateContent(
        streamId: StreamId,
        content: Collection<RawMessage.Builder>,
        streamData: StreamData?
    ) {
        var lastTime: Instant
        var lastSequence: Long
        if (streamData == null) {
            lastTime = Instant.MIN
            lastSequence = -1
        } else {
            lastTime = streamData.lastTimestamp
            lastSequence = streamData.lastSequence
        }

        content.forEach {
            val (currentTimestamp, curSequence) = it.checkTimeAndSequence(streamId, lastTime, lastSequence)
            lastTime = currentTimestamp
            lastSequence = curSequence
        }
        readerState[streamId] = StreamData(lastTime, lastSequence)
    }

    private fun RawMessage.Builder.checkTimeAndSequence(
        streamId: StreamId,
        lastTime: Instant,
        lastSequence: Long
    ): Pair<Instant, Long> {
        val currentTimestamp = metadata.timestamp.toInstant()
        if (currentTimestamp < lastTime) {
            fixOrAlert(streamId, metadataBuilder, lastTime)
        }
        val curSequence = metadata.id.sequence
        check(curSequence > lastSequence) {
            "The sequence does not increase monotonically. Last seq: $lastSequence; current seq: $curSequence"
        }
        return Pair(currentTimestamp, curSequence)
    }

    private fun fixOrAlert(streamId: StreamId, metadata: RawMessageMetadata.Builder, lastTime: Instant) {
        if (configuration.fixTimestamp) {
            LOGGER.debug { "Fixing timestamp for $streamId. Current: ${metadata.timestamp.toInstant()}; after fix: $lastTime" }
            metadata.timestamp = lastTime.toTimestamp()
        } else {
            throw IllegalStateException("The time does not increase monotonically. " +
                "Last timestamp: $lastTime; current timestamp: ${metadata.timestamp.toInstant()}")
        }
    }

    private fun holdersToProcess(): Map<StreamId, FileHolder<T>> {
        fileTracker.pollFileSystemEvents(10, TimeUnit.MILLISECONDS)
        val newFiles: Map<StreamId, Path> = pullUpdates()
        val streams = newFiles.keys + currentFilesByStreamId.keys

        val holdersByStreamId: MutableMap<StreamId, FileHolder<T>> = hashMapOf()
        for (streamId in streams) {
            val fileHolder = currentFilesByStreamId[streamId] ?: run {
                newFiles[streamId]?.toFileHolder(streamId)?.also {
                    currentFilesByStreamId[streamId] = it
                    onSourceFound(streamId, it.path)
                }
            }

            if (fileHolder == null) {
                LOGGER.trace { "Not data for $streamId. Wait for the next attempt" }
                continue
            }

            fileHolder.refreshFileInfo()
            if (!canReadRightNow(fileHolder, configuration.staleTimeout)) {
                LOGGER.debug { "Cannot read ${fileHolder.path} right now. Wait for the next attempt" }
                continue
            }

            if (!fileHolder.sourceWrapper.hasMoreData && !canBeClosed(streamId, fileHolder)) {
                LOGGER.debug {
                    "The ${fileHolder.path} file for stream $streamId cannot be closed yet and does not have any data. " +
                        "Wait for the next read attempt"
                }
                continue
            }

            holdersByStreamId[streamId] = fileHolder
        }
        return holdersByStreamId
    }

    private fun pullUpdates(): Map<StreamId, Path> = directoryChecker.check { streamId, path ->
        val fileHolder = currentFilesByStreamId[streamId]
        !readerState.isFileProcessed(path)
            && !readerState.isStreamIdExcluded(streamId)
            && isNotTheSameFile(path, fileHolder)
            && acceptFile(streamId, fileHolder?.path, path).also {
            LOGGER.trace { "Calling 'acceptFile' for $path (streamId: $streamId). Current file: ${fileHolder?.path}" }
        }
    }

    private fun isNotTheSameFile(
        path: Path,
        fileHolder: FileHolder<T>?
    ): Boolean {
        return fileHolder == null
            || (!fileHolder.isActual && fileHolder.path == path)
            || !fileTracker.isSameFiles(fileHolder.path, path)
    }

    protected class FileHolder<T : AutoCloseable>(
        val path: Path,
        private val sourceSupplier: (Path) -> FileSourceWrapper<T>
    ) : AutoCloseable {
        private val attributesView = Files.getFileAttributeView(path, BasicFileAttributeView::class.java)
        private var state: FileState = UNKNOWN_STATE
        private var _changed: Boolean = true
        private var _sourceWrapper: FileSourceWrapper<T>? = null
        private var _closed = false
        private var sourceState: State = State.ACTUAL

        init {
            refreshFileInfo()
        }

        val lastModificationTime: FileTime
            get() = state.lastModification
        val size: Long
            get() = state.fileSize
        val changed: Boolean
            get() = _changed
        val closed: Boolean
            get() = _closed
        val stillExist: Boolean
            get() = Files.exists(path)

        /**
         * The source for the holder is still in place and wos not moved or deleted
         */
        val isActual: Boolean
            get() = sourceState == State.ACTUAL

        internal val sourceWrapper: FileSourceWrapper<T>
            get() {
                check(!closed) { "Source for path $path already closed" }
                var tmp = _sourceWrapper
                if (tmp == null) {
                    tmp = sourceSupplier(path)
                    _sourceWrapper = tmp
                }
                return tmp
            }

        internal fun refreshFileInfo() {
            if (stillExist && isActual) {
                val prevState = state
                state = attributesView.readAttributes().toFileState()
                _changed = state != prevState
            }
        }

        override fun close() {
            try {
                _sourceWrapper?.close()
            } finally {
                _closed = true
            }
        }

        internal fun moved() {
            sourceTransferred(State.MOVED)
        }

        internal fun removed() {
            sourceTransferred(State.REMOVED)
        }

        private fun sourceTransferred(state: State) {
            sourceState = state
            _changed = false
        }

        override fun toString(): String {
            return "FileHolder(path=$path, " +
                "processState=$sourceState, " +
                "lastModificationTime=$lastModificationTime, " +
                "size=$size, " +
                "changed=$changed, " +
                "closed=$closed, " +
                "source=${_sourceWrapper?.run { "(hasMoreData=$hasMoreData)" }}" +
                ")"
        }

        private enum class State { ACTUAL, MOVED, REMOVED }

        companion object {
            private val UNKNOWN_STATE = FileState(FileTime.fromMillis(0), -1)
        }

    }

    protected data class FileState(
        val lastModification: FileTime,
        val fileSize: Long,
    )

    private fun Path.toFileHolder(streamId: StreamId): FileHolder<T> {
        return FileHolder(this) {
            LOGGER.info { "Opening source for $it file" }
            createSource(streamId, it)
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun BasicFileAttributes.toFileState() = FileState(lastModifiedTime(), size())
        val DEFAULT_SEQUENCE_GENERATOR: (StreamId) -> Long = { Instant.now().run { epochSecond * TimeUnit.SECONDS.toNanos(1) + nano } }
    }
}
