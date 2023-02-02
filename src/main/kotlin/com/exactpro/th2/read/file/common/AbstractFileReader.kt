/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
 *
 */

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadataOrBuilder
import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.toInstant
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import com.exactpro.th2.read.file.common.impl.DelegateReaderListener
import com.exactpro.th2.read.file.common.metric.FilesMetric
import com.exactpro.th2.read.file.common.metric.ReaderMetric
import com.exactpro.th2.read.file.common.recovery.RecoverableException
import com.exactpro.th2.read.file.common.recovery.RecoverableFileSourceWrapper
import com.exactpro.th2.read.file.common.state.GroupData
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.StreamData
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.math.abs

abstract class AbstractFileReader<T : AutoCloseable, K : DataGroupKey>(
    private val configuration: CommonFileReaderConfiguration,
    private val directoryChecker: DirectoryChecker<K>,
    private val contentParser: ContentParser<T, K>,
    private val readerState: ReaderState<K>,
    private val readerListener: ReaderListener<K>,
    private val sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR,
    private val messageFilters: Collection<ReadMessageFilter> = emptyList(),
) : AutoCloseable {
    constructor(
        configuration: CommonFileReaderConfiguration,
        directoryChecker: DirectoryChecker<K>,
        contentParser: ContentParser<T, K>,
        readerState: ReaderState<K>,
        onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit,
        onError: (K?, String, Exception) -> Unit = { _, _, _ -> },
        sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR
    ) : this(
        configuration,
        directoryChecker,
        contentParser,
        readerState,
        DelegateReaderListener(onStreamData, onError),
        sequenceGenerator
    )

    @Volatile
    private var closed: Boolean = false

    private val currentFilesByDataGroup: MutableMap<K, FileHolder<T>> = ConcurrentHashMap()
    private val contentByDataGroup: MutableMap<K, MutableMap<StreamId, PublicationHolder>> = ConcurrentHashMap()
    private val pendingGroups: MutableSet<K> = ConcurrentHashMap.newKeySet()
    @Volatile
    private var cachedUpdates: Map<K, Path> = emptyMap()
    @Volatile
    private var lastPullUpdates: Instant = Instant.MIN

    private lateinit var fileTracker: MovedFileTracker
    private val trackerListener = object : MovedFileTracker.FileTrackerListener {
        override fun moved(prev: Path, current: Path) {
            val entry = currentFilesByDataGroup.entries.find { it.value.path == prev }
            if (entry != null) {
                val (group, holderWithPathMatch) = entry
                LOGGER.info { "File $prev moved to $current" }
                holderWithPathMatch.moved()
                readerState.fileProcessed(group, current)
            } else {
                if (readerState.fileMoved(prev, current)) {
                    LOGGER.info { "Already processed $prev file was moved to $current. Update processed files list" }
                }
            }
        }

        override fun removed(paths: Set<Path>) {
            readerState.processedFilesRemoved(paths)
            val holderWithRemovedFiles = currentFilesByDataGroup.values
                .filter { paths.contains(it.path) }
            if (holderWithRemovedFiles.isEmpty()) {
                return
            }
            LOGGER.info { "Files removed: ${holderWithRemovedFiles.joinToString(", ") { it.path.toString() }}" }
            holderWithRemovedFiles.forEach { it.removed() }
        }
    }

    private class PublicationHolder {
        private var _startOfPublishing: Long? = null
        private var _batchesPublished: Int = 0
        private var _creationTime: Instant = Instant.now()
        val creationTime: Instant
            get() = _creationTime

        /**
         * Call on batch publication to increment the counter
         */
        fun published() {
            if (_startOfPublishing == null) {
                _startOfPublishing = System.currentTimeMillis()
            }
            _batchesPublished++
        }

        fun isLimitExceeded(publicationPerSecond: Int): Boolean {
            val startPublishing = _startOfPublishing
            return startPublishing != null && millisSinceStartPublishing(startPublishing) < 1_000 && _batchesPublished >= publicationPerSecond
        }

        fun isTimeToReset(): Boolean {
            val publishedAt = _startOfPublishing
            return publishedAt != null && millisSinceStartPublishing(publishedAt) > 1_000
        }

        /**
         * Reset the information to calculate publication limit
         */
        fun resetLimit() {
            _startOfPublishing = null
            _batchesPublished = 0
        }

        /**
         * Do not forget to copy the content before passing it to anywhere
         */
        val content: MutableList<RawMessage.Builder> = arrayListOf()

        fun resetCurrent() {
            _creationTime = Instant.now()
            content.clear()
        }

        private fun millisSinceStartPublishing(startPublishing: Long) = abs(System.currentTimeMillis() - startPublishing)

        override fun toString(): String {
            return "PublicationHolder(" +
                "startOfPublishing=$_startOfPublishing, " +
                "batchesPublished=$_batchesPublished, " +
                "creationTime=$_creationTime, " +
                "content=${content.size}" +
                ")"
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
            var holdersByGroup: Map<K, FileHolder<T>> = emptyMap()
            var updateRequired = true
            do {
                if (updateRequired) {
                    holdersByGroup = ReaderMetric.measurePulling { holdersToProcess() }
                    lastPullUpdates = Instant.now()
                    updateRequired = false
                }
                LOGGER.debug { "Get ${holdersByGroup.size} holder(s) to process" }
                for ((group, fileHolder) in holdersByGroup) {
                    val holderProcessed = ReaderMetric.measureReading { processHolderForGroup(group, fileHolder) }
                    updateRequired = updateRequired or holderProcessed
                }

                for ((_, holderByStream) in contentByDataGroup) {
                    for ((streamId, holder) in holderByStream) {
                        if (!configuration.unlimitedPublication
                            && isPublicationLimitExceeded(streamId, holder, configuration.maxBatchesPerSecond)
                        ) {
                            continue
                        }
                        holder.tryToPublish(streamId)
                    }
                }
                updateRequired = updateRequired or (Duration.between(lastPullUpdates, Instant.now()) >= configuration.minDelayBetweenUpdates)
            } while (holdersByGroup.isNotEmpty() && !Thread.currentThread().isInterrupted)
            LOGGER.debug { "Checking finished" }
        } catch (ex: TruncatedSourceException) {
            LOGGER.error(ex) { "Source was truncated but it is not allowed by configuration" }
            readerListener.onError(ex.castedGroup(), "Source was truncated but it is not allowed by configuration", ex)
            failStreamId(ex.castedGroup(), null, ex.holder, ex)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Error during processing updates" }
            readerListener.onError(null, "Error during processing updates", ex)
            if (ex is InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    /**
     * Returns `true` if current file was fully processed (normally or with error)
     */
    private fun processHolderForGroup(
        dataGroup: K,
        fileHolder: FileHolder<T>,
    ): Boolean {
        val groupData: GroupData? = readerState[dataGroup]
        LOGGER.trace { "Processing holder for $dataGroup ($groupData). $fileHolder" }
        if (checkFileDrop(fileHolder, dataGroup, groupData)) {
            return true
        }

        if (isAnyPublicationLimitExceeded(dataGroup)) {
            LOGGER.trace { "The publication limit in ${configuration.maxBatchesPerSecond} batch/s for $dataGroup exceeded. Suspend reading" }
            return false
        }

        val readContent: Map<StreamId, Collection<RawMessage.Builder>> = try {
            readMessages(dataGroup, fileHolder)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Error during reading messages for $dataGroup. File holder: $fileHolder" }
            readerListener.onError(dataGroup, "Cannot read data from the file ${fileHolder.path}", ex)
            failStreamId(dataGroup, null, fileHolder, ex)
            return true
        }

        val sourceWrapper: FileSourceWrapper<T> = fileHolder.sourceWrapper
        if (readContent.isEmpty()) {
            if (!sourceWrapper.hasMoreData) {
                closeSourceIfAllowed(dataGroup, fileHolder)
            }
            return true
        }

        var fullyProcessed = false
        for ((streamId, messages) in readContent) {
            val streamData: StreamData? = readerState[streamId]
            val finalContent = onContentRead(dataGroup, streamId, fileHolder.path, messages)

            if (finalContent.isEmpty()) {
                LOGGER.trace { "No data to process after 'onContentRead' call for $streamId, current state: ${fileHolder.readState}" }
                continue // try next stream ID
            }

            val filteredContent: Collection<RawMessage.Builder> = finalContent.filterReadContent(streamId, streamData)
            if (filteredContent.isEmpty()) {
                LOGGER.trace { "No content messages left for $streamId in $dataGroup group after filtering" }
                continue // try next stream ID
            }
            if (!configuration.leaveLastFileOpen) {
                filteredContent.markMessagesWithTag(fileHolder)
            }
            setCommonInformation(streamId, filteredContent, streamData)
            try {
                validateContent(streamId, filteredContent, streamData)
            } catch (ex: Exception) {
                LOGGER.error(ex) {
                    "Failed to validate content for stream $streamId in group $dataGroup ($fileHolder):" +
                        " ${filteredContent.joinToString { shortDebugString(it) }}"
                }
                failStreamId(dataGroup, streamId, fileHolder, ex)
                fullyProcessed = true // because it caused an error, and we cannot safely continue reading
                continue // try next stream ID
            }
            tryPublishContent(dataGroup, streamId, filteredContent)
        }
        return fullyProcessed
    }

    private fun checkFileDrop(
        fileHolder: FileHolder<T>,
        groupKey: K,
        groupData: GroupData?,
    ): Boolean {
        val fileInfo = FilterFileInfo(fileHolder.path, fileHolder.lastModificationTime.toInstant(), configuration.staleTimeout)
        val filter = messageFilters.find { it.drop(groupKey, fileInfo, groupData) }
        if (filter != null) {
            LOGGER.info { "Source $fileInfo is dropped by filter ${filter::class.simpleName}. Group data: $groupData" }
            closeSourceIfAllowed(groupKey, fileHolder, FilesMetric.ProcessStatus.DROPPED)
            return true
        }
        return false
    }

    private fun Collection<RawMessage.Builder>.markMessagesWithTag(fileHolder: FileHolder<T>) {
        val lastState = fileHolder.readState
        fileHolder.updateState()

        if (size == 1 && lastState == FileHolder.ReadState.START && fileHolder.readState == FileHolder.ReadState.FIN) {
            first().markSingle()
        } else {
            if (lastState == FileHolder.ReadState.START) first().markFirst()
            if (fileHolder.readState == FileHolder.ReadState.FIN) last().markLast()
        }
    }

    private fun Collection<RawMessage.Builder>.filterReadContent(
        streamId: StreamId,
        streamData: StreamData?,
    ): Collection<RawMessage.Builder> = filter { msg ->
        val filter = messageFilters.find { it.drop(streamId, msg, streamData) }
        if (filter != null) {
            LOGGER.debug { "Content '${msg.toShortString()}' in $streamId stream was filtered by ${filter::class.java.simpleName} filter. Stream data: $streamData" }
        }
        filter == null
    }

    private fun RawMessage.Builder.markFirst(): RawMessageMetadata.Builder = metadataBuilder.putProperties(MESSAGE_STATUS_PROPERTY, MESSAGE_STATUS_FIRST)
    private fun RawMessage.Builder.markLast(): RawMessageMetadata.Builder = metadataBuilder.putProperties(MESSAGE_STATUS_PROPERTY, MESSAGE_STATUS_LAST)
    private fun RawMessage.Builder.markSingle(): RawMessageMetadata.Builder = metadataBuilder.putProperties(MESSAGE_STATUS_PROPERTY, MESSAGE_STATUS_SINGLE)


    override fun close() {
        if (closed) {
            LOGGER.warn { "Reader already closed" }
            return
        }
        try {
            contentByDataGroup.forEach { (_, holders) ->
                holders.forEach { (streamId, holder) ->
                    with(holder) {
                        if (content.isNotEmpty()) {
                            publish(streamId)
                        }
                    }
                }
            }
            currentFilesByDataGroup.forEach(this::closeSource)
            if (::fileTracker.isInitialized) {
                fileTracker -= trackerListener
            }
        } finally {
            closed = true
        }
    }

    protected open fun canBeClosed(dataGroup: K, fileHolder: FileHolder<T>): Boolean {
        val canCloseTheLastFile = canCloseTheLastFileFor(dataGroup, fileHolder)
        return (canCloseTheLastFile && noChangesForStaleTimeout(fileHolder))
            || !fileHolder.isActual
            || !fileHolder.stillExist
            || canForceFileClosing(dataGroup, fileHolder, fileHolder.sourceWrapper)
            || fileHolder.isFileEndReached
    }

    /**
     * The method is invoked when the read checks whether the file can be closed or not.
     * Can force closing by custom conditions
     * @return whether the file should be closed or not
     */
    protected open fun canForceFileClosing(
        dataGroup: K,
        fileHolder: FileHolder<T>,
        sourceWrapper: FileSourceWrapper<T>
    ): Boolean = false

    /**
     * Will be invoked when the new source file for [dataGroup] is found.
     */
    protected open fun onSourceFound(
        dataGroup: K,
        path: Path,
    ) {
        // do nothing
    }

    /**
     * Will be invoked on each read content.
     * Can be used to modify the [RawMessage.Builder] before publishing them
     */
    protected open fun onContentRead(
        dataGroup: K,
        streamId: StreamId,
        path: Path,
        readContent: Collection<RawMessage.Builder>,
    ): Collection<RawMessage.Builder> {
        return readContent
    }

    /**
     * Will be invoked when an error is accurate during processing the source from [path] file for [DataGroupKey]
     */
    protected open fun onSourceCorrupted(
        dataGroup: K,
        path: Path,
        cause: Exception,
    ) {
        // do nothing
    }

    /**
     * Will be invoked when the source if finished and is closed
     */
    protected open fun onSourceClosed(
        dataGroup: K,
        path: Path,
    ) {
        // do nothing
    }

    protected abstract fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean

    protected abstract fun acceptFile(dataGroup: K, currentFile: Path?, newFile: Path): Boolean

    protected abstract fun createSource(dataGroup: K, path: Path): FileSourceWrapper<T>

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

    protected fun noChangesForStaleTimeout(fileHolder: FileHolder<T>): Boolean = !fileHolder.changed &&
        abs(System.currentTimeMillis() - fileHolder.lastModificationTime.toMillis()) > configuration.staleTimeout.toMillis()

    protected fun canCloseTheLastFileFor(dataGroup: K, holder: FileHolder<T>): Boolean {
        return if (configuration.leaveLastFileOpen) {
            hasNewFilesFor(dataGroup, holder)
        } else {
            true
        }
    }

    private fun hasNewFilesFor(dataGroup: K, holder: FileHolder<T>): Boolean =
        pullUpdates()[dataGroup]?.let { isNotTheSameFile(it, holder) } ?: false

    private fun tryPublishContent(
        groupKey: K,
        streamId: StreamId,
        readContent: Collection<RawMessage.Builder>
    ) {
        val publicationHolder = contentByDataGroup.computeIfAbsent(groupKey) { ConcurrentHashMap() }
            .computeIfAbsent(streamId) { PublicationHolder() }
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
            resetCurrent()
            return
        }
        val size = content.size
        val newSize = size + addToBatch
        val maxBatchSize = configuration.maxBatchSize
        if (newSize > maxBatchSize || size == maxBatchSize || timeForPublication(creationTime)) {
            LOGGER.debug { "Publish the content for stream $streamId. Content size: $size; Creation time: $creationTime" }
            publish(streamId)
        }
    }

    private fun PublicationHolder.publish(streamId: StreamId) {
        readerListener.onStreamData(streamId, content.toList())
        published()
        resetCurrent()
    }

    private fun timeForPublication(creationTime: Instant): Boolean {
        return Duration.between(creationTime, Instant.now()).abs() > configuration.maxPublicationDelay
    }

    private fun closeSourceIfAllowed(
        groupData: K,
        fileHolder: FileHolder<T>,
        terminalStatus: FilesMetric.ProcessStatus = FilesMetric.ProcessStatus.PROCESSED,
    ) {
        val path = fileHolder.path
        LOGGER.debug { "Source for $path file does not have any additional data yet. Check if we can close it" }
        if (canBeClosed(groupData, fileHolder)) {
            FilesMetric.incStatus(terminalStatus)
            terminateSource(groupData, fileHolder)
            onSourceClosed(groupData, fileHolder.path)
        }
    }

    private fun failStreamId(
        dataGroup: K,
        streamId: StreamId?,
        fileHolder: FileHolder<*>,
        cause: Exception
    ) {
        LOGGER.debug { "Terminating source from file ${fileHolder.path} for data group $dataGroup (stream: $streamId)" }
        terminateSource(dataGroup, fileHolder)
        if (configuration.continueOnFailure) {
            LOGGER.warn { "Continue processing files for stream $streamId ignoring error when reading file ${fileHolder.path}: $cause" }
            if (fileHolder.stillExist) {
                readerState.fileProcessed(dataGroup, fileHolder.path)
            }
        } else {
            readerState.excludeDataGroup(dataGroup)
        }
        FilesMetric.incStatus(FilesMetric.ProcessStatus.ERROR)
        onSourceCorrupted(dataGroup, fileHolder.path, cause)
    }

    private fun terminateSource(
        dataGroup: K,
        fileHolder: FileHolder<*>,
    ) {
        closeSource(dataGroup, fileHolder)
        currentFilesByDataGroup.remove(dataGroup)
        if (fileHolder.isActual) {
            readerState.fileProcessed(dataGroup, fileHolder.path)
        }
    }

    private fun closeSource(dataGroup: K, fileHolder: FileHolder<*>) {
        val prevData: GroupData? = readerState[dataGroup]
        val lastSourceModificationTimestamp = fileHolder.lastModificationTime.toInstant()
        readerState[dataGroup] = prevData?.copy(lastSourceModificationTimestamp = lastSourceModificationTimestamp)
            ?: GroupData(lastSourceModificationTimestamp)
        val path = fileHolder.path
        LOGGER.info { "Closing source for $path file" }
        runCatching { fileHolder.close() }
            .onSuccess { LOGGER.debug { "Source for file $path successfully closed" } }
            .onFailure { LOGGER.error(it) { "Cannot close source for file $path" } }
    }

    private fun readMessages(
        dataGroup: K,
        holder: FileHolder<T>
    ): Map<StreamId, Collection<RawMessage.Builder>> {
        if (!holder.sourceWrapper.hasMoreData) {
            return emptyMap()
        }
        var content: Map<StreamId, Collection<RawMessage.Builder>> = emptyMap()

        with(holder.sourceWrapper) {
            do {
                mark()
                val canParse: Boolean = try {
                    contentParser.canParse(dataGroup, source, noChangesForStaleTimeout(holder)).also {
                        reset()
                    }
                } catch (ex: RecoverableException) {
                    LOGGER.debug(ex) { "The source for $dataGroup (${holder.path}) requires to be reopen and recovered" }
                    if (!holder.supportRecovery) {
                        LOGGER.error { "Recovery is not supported by the source ${holder.sourceWrapper::class.qualifiedName} for file ${holder.path}" }
                        throw ex
                    }
                    reset()
                    holder.recoverSource()
                    false
                }
                if (canParse) {
                    content = contentParser.parse(dataGroup, source)
                    if (content.isNotEmpty()) {
                        LOGGER.trace { "Read ${content.size} message(s) for $dataGroup from ${holder.path}" }
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
        readerState[streamId] = StreamData(lastTime, lastSequence, content.last().body)
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

    private fun holdersToProcess(): Map<K, FileHolder<T>> {
        LOGGER.debug { "Collecting holders to process" }
        if (!configuration.disableFileMovementTracking) {
            LOGGER.debug { "Pulling file system events" }
            fileTracker.pollFileSystemEvents(10, TimeUnit.MILLISECONDS)
        }
        val newFiles: Map<K, Path> = pullUpdates(useCache = false)
        LOGGER.debug { "New files: $newFiles" }
        val dataGroups = newFiles.keys + currentFilesByDataGroup.keys

        val sharedFiles: Map<Path, Set<K>> = newFiles.entries.groupBy(
            Map.Entry<K, Path>::value,
            Map.Entry<K, Path>::key,
        ).mapValues {
            it.value.toSet() - currentFilesByDataGroup.keys
        }.filterValues { it.isNotEmpty() }
        if (sharedFiles.isNotEmpty()) {
            LOGGER.debug { "Shared files: $sharedFiles" }
        }

        val holdersByStreamId: MutableMap<K, FileHolder<T>> = hashMapOf()
        for (group in dataGroups) {
            LOGGER.debug { "Checking holder for $group" }
            val fileHolder = currentFilesByDataGroup[group] ?: run {
                newFiles[group]?.toFileHolder(group)?.also {
                    currentFilesByDataGroup[group] = it
                    sourceFound(group, it)
                }
            }

            if (fileHolder == null) {
                LOGGER.trace { "No data for $group. Wait for the next attempt" }
                continue
            }

            LOGGER.debug { "Refreshing file info for $group ($fileHolder)" }
            fileHolder.refreshFileInfo()
            if (fileHolder.truncated) {
                if (!configuration.allowFileTruncate) {
                    throw TruncatedSourceException(group, fileHolder)
                }
                LOGGER.info { "File ${fileHolder.path} was truncated. Start reading from the beginning" }
                fileHolder.reopen()
            }
            LOGGER.debug { "Checking if can read from source for $group ($fileHolder)" }
            if (!canReadRightNow(fileHolder, configuration.staleTimeout)) {
                if (addToPending(group)) {
                    LOGGER.debug { "Cannot read $fileHolder right now. Wait for the next attempt" }
                } else {
                    LOGGER.trace { "Still cannot read ${fileHolder.path} for stream $group. Wait for next attempt" }
                }
                continue
            }

            LOGGER.debug { "Checking if ${fileHolder.path} source can be closed for stream $group" }
            if (!fileHolder.sourceWrapper.hasMoreData && !canBeClosed(group, fileHolder)) {
                if (addToPending(group)) {
                    LOGGER.debug {
                        "The ${fileHolder.path} file for stream $group cannot be closed yet and does not have any data. " +
                            "Wait for the next read attempt"
                    }
                } else {
                    LOGGER.trace {
                        "The ${fileHolder.path} file for stream $group cannot be closed yet and still does not have any data. " +
                            "Wait for the next read attempt"
                    }
                }
                continue
            }

            holdersByStreamId[group] = fileHolder
        }
        removeFromPending(holdersByStreamId.keys)
        return holdersByStreamId
    }

    private fun sourceFound(dataGroup: K, it: FileHolder<T>) {
        FilesMetric.incStatus(FilesMetric.ProcessStatus.FOUND)
        onSourceFound(dataGroup, it.path)
    }

    private fun addToPending(id: K): Boolean {
        return pendingGroups.add(id)
    }

    private fun removeFromPending(ids: Set<K>) {
        pendingGroups.removeAll(ids)
    }

    private fun isAnyPublicationLimitExceeded(dataGroup: K): Boolean {
        val limit = configuration.maxBatchesPerSecond
        // fast way if no limit
        if (configuration.unlimitedPublication) {
            return false
        }
        val holdersByStreamId: Map<StreamId, PublicationHolder> = contentByDataGroup[dataGroup] ?: return false
        for ((streamId, holder) in holdersByStreamId) {
            if (isPublicationLimitExceeded(streamId, holder, limit)) {
                return true
            }
        }
        return false
    }

    private fun isPublicationLimitExceeded(
        streamId: StreamId,
        publicationHolder: PublicationHolder,
        limit: Int
    ): Boolean {
        if (publicationHolder.isTimeToReset()) {
            LOGGER.trace { "Reset time limit for $streamId ($publicationHolder)" }
            publicationHolder.resetLimit()
        }
        return publicationHolder.isLimitExceeded(limit)
    }

    private fun pullUpdates(useCache: Boolean = true): Map<K, Path> {
        if (useCache && cachedUpdates.isNotEmpty()) {
            return cachedUpdates
        }
        return directoryChecker.check { dataGroup, path ->
            val fileHolder = currentFilesByDataGroup[dataGroup]
            when {
                readerState.isFileProcessed(dataGroup, path) -> false
                readerState.isDataGroupExcluded(dataGroup) -> {
                    LOGGER.warn { "DataGroup $dataGroup is excluded from further processing" }
                    FilesMetric.incStatus(FilesMetric.ProcessStatus.DROPPED)
                    readerState.fileProcessed(dataGroup, path)
                    false
                }
                else -> isNotTheSameFile(path, fileHolder) && acceptFile(dataGroup, fileHolder?.path, path).also {
                    LOGGER.trace { "Calling 'acceptFile' for $path (dataGroup: $dataGroup). Current file: ${fileHolder?.path} with result $it" }
                }
            }
        }.also {
            cachedUpdates = it
        }
    }

    private fun isNotTheSameFile(
        path: Path,
        fileHolder: FileHolder<T>?
    ): Boolean {
        return fileHolder == null
            || (!fileHolder.isActual && fileHolder.path == path)
            || (if (configuration.disableFileMovementTracking) fileHolder.path != path else !fileTracker.isSameFiles(fileHolder.path, path))
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
        var truncated: Boolean = false
            private set
        var readState: ReadState = ReadState.START
            private set

        /**
         * The source for the holder is still in place and wos not moved or deleted
         */
        val isActual: Boolean
            get() = sourceState == State.ACTUAL

        val supportRecovery: Boolean
            get() = _sourceWrapper is RecoverableFileSourceWrapper

        internal val isFileEndReached: Boolean
            get() = (_sourceWrapper as? EndAwareFileSourceWrapper)?.fileEndReached ?: false

        internal fun updateState() {
            readState = if (sourceWrapper.hasMoreData) {
                ReadState.IN_PROGRESS
            } else {
                ReadState.FIN
            }
            LOGGER.trace { "Update state for: $this" }
        }

        internal fun recoverSource() {
            check(!closed) { "Source for path $path already closed" }
            val wrapper = _sourceWrapper
            check(wrapper is RecoverableFileSourceWrapper) { "Source wrapper is not an instance of ${RecoverableFileSourceWrapper::class.qualifiedName}" }
            check(stillExist) { "File $path does not exist anymore" }
            _sourceWrapper = wrapper.recoverFrom(sourceSupplier(path).source)
            LOGGER.trace { "Closing the previous wrapper" }
            runCatching { wrapper.close() }
                .onSuccess { LOGGER.trace { "The previous wrapper successfully closed" } }
                .onFailure { LOGGER.error(it) { "Cannot close the previous source wrapper" } }
        }

        internal fun reopen() {
            check(!closed) { "Source for path $path already closed" }
            val wrapper = _sourceWrapper
            checkNotNull(wrapper) { "The original wrapper for file $path is not created yet" }
            check(stillExist) { "File $path does not exist anymore" }
            _sourceWrapper = sourceSupplier(path)
            runCatching { wrapper.close() }
                .onSuccess { LOGGER.trace { "The previous wrapper successfully closed" } }
                .onFailure { LOGGER.error(it) { "Cannot close the previous source wrapper" } }
            readState = ReadState.START
            refreshFileInfo()
        }

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
                truncated = prevState.fileSize > state.fileSize
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
                "readState=$readState, " +
                "source=${_sourceWrapper?.run { "(hasMoreData=$hasMoreData)" }}" +
                ")"
        }

        private enum class State { ACTUAL, MOVED, REMOVED }

        enum class ReadState { START, IN_PROGRESS, FIN }

        companion object {
            private val UNKNOWN_STATE = FileState(FileTime.fromMillis(0), -1)
        }

    }

    protected data class FileState(
        val lastModification: FileTime,
        val fileSize: Long,
    )

    private class TruncatedSourceException(
        val dataGroup: DataGroupKey,
        val holder: FileHolder<*>
    ) : Exception() {
        override val message: String = "File ${holder.path} for stream ID $dataGroup was truncated"

        companion object {
            private const val serialVersionUID: Long = -242207363318682233L
        }
    }

    @Suppress("UNCHECKED_CAST") // should never happen
    private fun <T : DataGroupKey> TruncatedSourceException.castedGroup(): T = dataGroup as T

    private fun Path.toFileHolder(dataGroup: K): FileHolder<T>? {
        return try {
            FileHolder(this) {
                LOGGER.info { "Opening source for $it file" }
                createSource(dataGroup, it)
            }
        } catch (ex: NoSuchFileException) {
            LOGGER.error(ex) { "Cannot create holder for $this because file does not exist anymore" }
            null
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        const val MESSAGE_STATUS_PROPERTY = "th2.read.order_marker"
        const val MESSAGE_STATUS_SINGLE = "single"
        const val MESSAGE_STATUS_FIRST = "start"
        const val MESSAGE_STATUS_LAST = "fin"

        private fun BasicFileAttributes.toFileState() = FileState(lastModifiedTime(), size())
        private val CommonFileReaderConfiguration.unlimitedPublication: Boolean
            get() = maxBatchesPerSecond == UNLIMITED_PUBLICATION

        val DEFAULT_SEQUENCE_GENERATOR: (StreamId) -> Long = { Instant.now().run { epochSecond * TimeUnit.SECONDS.toNanos(1) + nano } }
        const val UNLIMITED_PUBLICATION = -1
    }
}

private fun RawMessageOrBuilder.toShortString(): String {
    return "timestamp: ${metadata.timestampOrNull?.toInstant()}; size: ${body.size()}"
}

private val RawMessageMetadataOrBuilder.timestampOrNull: Timestamp?
    get() = if (hasTimestamp()) timestamp else null