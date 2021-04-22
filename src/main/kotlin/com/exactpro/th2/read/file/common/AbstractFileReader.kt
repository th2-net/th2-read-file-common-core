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
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.abs

abstract class AbstractFileReader<T : AutoCloseable>(
    private val configuration: CommonFileReaderConfiguration,
    private val directoryChecker: DirectoryChecker,
    private val contentParser: ContentParser<T>,
    private val onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit
) : AutoCloseable {
    @Volatile
    private var closed: Boolean = false

    private val currentFilesByStreamId: MutableMap<StreamId, FileHolder<T>> = ConcurrentHashMap()
    private val contentByStreamId: MutableMap<StreamId, PublicationHolder> = ConcurrentHashMap()
    private val processedFiles: MutableSet<Path> = ConcurrentHashMap.newKeySet()

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

    fun processUpdates() {
        check(!closed) { "Reader already closed" }
        LOGGER.debug { "Checking updates" }
        do {
            val holdersByStreamId: Map<StreamId, FileHolder<T>> = holdersToProcess()
            LOGGER.trace { "Get ${holdersByStreamId.size} holder(s) to process" }
            for ((streamId, fileHolder) in holdersByStreamId) {
                val sourceWrapper: FileSourceWrapper<T> = fileHolder.sourceWrapper
                val readContent: Collection<RawMessage.Builder> = if (sourceWrapper.hasMoreData) {
                    readMessages(streamId, fileHolder)
                } else {
                    emptyList()
                }

                if (readContent.isEmpty()) {
                    if (!sourceWrapper.hasMoreData) {
                        closeSourceIfAllowed(fileHolder, streamId)
                    }
                    continue
                }

                onContentRead(streamId, fileHolder, readContent)

                tryPublishContent(streamId, readContent)
            }

            for ((streamId, holder) in contentByStreamId) {
                holder.tryToPublish(streamId)
            }
        } while (holdersByStreamId.isNotEmpty() && !Thread.currentThread().isInterrupted)
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
        } finally {
            closed = true
        }
    }

    protected open fun canBeClosed(streamId: StreamId, fileHolder: FileHolder<T>): Boolean {
        val canCloseTheLastFile = canCloseTheLastFileFor(streamId)
        return canCloseTheLastFile && noChangesForStaleTimeout(fileHolder)
    }

    /**
     * Will be invoke on each read content.
     * Can be used to modify the [RawMessage.Builder] before publishing them
     */
    protected open fun onContentRead(
        streamId: StreamId,
        fileHolder: FileHolder<T>,
        readContent: Collection<RawMessage.Builder>
    ) {
        readContent.forEach {
            it.metadataBuilder.idBuilder.apply {
                connectionIdBuilder.sessionAlias = streamId.sessionAlias
                direction = streamId.direction
            }
        }
    }

    /**
     * Will be invoked when the source if finished and is closed
     */
    protected open fun onSourceClosed(
        streamId: StreamId,
        path: Path
    ) {
        // do nothing
    }

    protected abstract fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean

    protected abstract fun acceptFile(streamId: StreamId, currentFile: Path?, newFile: Path): Boolean

    protected abstract fun createSource(streamId: StreamId, path: Path): FileSourceWrapper<T>

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
            LOGGER.trace { "Publish the content for stream $streamId. Content size: ${content.size}; Creation time: $creationTime" }
            publish(streamId)
        }
    }

    private fun PublicationHolder.publish(streamId: StreamId) {
        onStreamData(streamId, content.toList())
        reset()
    }

    private fun timeForPublication(creationTime: Instant): Boolean {
        return Duration.between(creationTime, Instant.now()).abs().toMillis() > configuration.maxPublicationDelay.toMillis()
    }

    private fun closeSourceIfAllowed(
        fileHolder: FileHolder<T>,
        streamId: StreamId
    ) {
        val path = fileHolder.path
        LOGGER.debug { "Source for $path file does not have any additional data yet. Check if we can close it" }
        if (canBeClosed(streamId, fileHolder)) {
            closeSource(fileHolder)
            currentFilesByStreamId.remove(streamId)
            processedFiles.add(fileHolder.path)
            onSourceClosed(streamId, fileHolder.path)
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
        var content: Collection<RawMessage.Builder> = emptyList()

        with(holder.sourceWrapper) {
            var canParse: Boolean
            do {
                mark()
                canParse = contentParser.canParse(streamId, source, noChangesForStaleTimeout(holder))
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

    private fun holdersToProcess(): Map<StreamId, FileHolder<T>> {
        val newFiles: Map<StreamId, Path> = pullUpdates()
        val streams = newFiles.keys + currentFilesByStreamId.keys

        val holdersByStreamId: MutableMap<StreamId, FileHolder<T>> = hashMapOf()
        for (streamId in streams) {
            val fileHolder = currentFilesByStreamId[streamId] ?: run {
                newFiles[streamId]?.toFileHolder(streamId)?.also {
                    currentFilesByStreamId[streamId] = it
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

    protected fun pullUpdates(): Map<StreamId, Path> = directoryChecker.check { streamId, path ->
        val fileHolder = currentFilesByStreamId[streamId]
        !processedFiles.contains(path) && isNotTheSameFile(path, fileHolder) && acceptFile(streamId, fileHolder?.path, path).also {
            LOGGER.trace { "Calling 'acceptFile' for $path (streamId: $streamId). Current file: ${fileHolder?.path}" }
        }
    }

    private fun isNotTheSameFile(
        path: Path,
        fileHolder: FileHolder<T>?
    ): Boolean {
        return fileHolder == null
            || path != fileHolder.path
        // TODO: need to check the files that was created with the same name
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
            val prevState = state
            state = attributesView.readAttributes().toFileState()
            _changed = state != prevState
        }

        override fun close() {
            try {
                _sourceWrapper?.close()
            } finally {
                _closed = true
            }
        }

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

    protected val Path.attributes: BasicFileAttributes
        get() = Files.readAttributes(this, BasicFileAttributes::class.java)

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun BasicFileAttributes.toFileState() = FileState(lastModifiedTime(), size())
    }
}
