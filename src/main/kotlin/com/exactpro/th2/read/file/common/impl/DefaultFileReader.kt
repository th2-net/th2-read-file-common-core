/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.AbstractFileReader
import com.exactpro.th2.read.file.common.ContentParser
import com.exactpro.th2.read.file.common.DirectoryChecker
import com.exactpro.th2.read.file.common.FileSourceWrapper
import com.exactpro.th2.read.file.common.MovedFileTracker
import com.exactpro.th2.read.file.common.ReadMessageFilter
import com.exactpro.th2.read.file.common.ReaderListener
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.attributes
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.impl.InMemoryReaderState
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import kotlin.math.abs

class DefaultFileReader<T : AutoCloseable> private constructor(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker,
    contentParser: ContentParser<T>,
    readerState: ReaderState,
    readerListener: ReaderListener,
    private val delegateHolder: DelegateHolder<T>,
    sequenceGenerator: (StreamId) -> Long,
    private val sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>,
    messageFilters: Collection<ReadMessageFilter>,
) : AbstractFileReader<T>(
    configuration,
    directoryChecker,
    contentParser,
    readerState,
    readerListener,
    sequenceGenerator,
    messageFilters,
) {
    override fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean = delegateHolder.canRead(holder, staleTimeout)

    override fun acceptFile(streamId: StreamId, currentFile: Path?, newFile: Path): Boolean = delegateHolder.acceptFile(streamId, currentFile, newFile)

    override fun createSource(streamId: StreamId, path: Path): FileSourceWrapper<T> = sourceFactory(streamId, path)

    override fun onSourceFound(streamId: StreamId, path: Path) {
        delegateHolder.onSourceFound.invoke(streamId, path)
    }

    override fun onContentRead(streamId: StreamId, path: Path, readContent: Collection<RawMessage.Builder>): Collection<RawMessage.Builder> {
        return delegateHolder.onContentRead.invoke(streamId, path, readContent)
    }

    override fun onSourceCorrupted(streamId: StreamId, path: Path, cause: Exception) {
        delegateHolder.onSourceCorrupted.invoke(streamId, path, cause)
    }

    override fun onSourceClosed(streamId: StreamId, path: Path) {
        delegateHolder.onSourceClosed.invoke(streamId, path)
    }

    private data class DelegateHolder<T : AutoCloseable>(
        val canRead: (FileHolder<T>, Duration) -> Boolean = READ_AFTER_STALE_TIMEOUT,
        val acceptFile: (StreamId, Path?, Path) -> Boolean = { _, _, _ -> true },
        val onSourceFound: (StreamId, Path) -> Unit = { _, _ -> },
        val onContentRead: (StreamId, Path, Collection<RawMessage.Builder>) -> Collection<RawMessage.Builder> = { _, _, msgs -> msgs },
        val onSourceCorrupted: (StreamId, Path, Exception) -> Unit = { _, _, _ -> },
        val onSourceClosed: (StreamId, Path) -> Unit = { _, _ -> },
    )

    class Builder<T : AutoCloseable>(
        private val configuration: CommonFileReaderConfiguration,
        private val directoryChecker: DirectoryChecker,
        private val contentParser: ContentParser<T>,
        private val fileTracker: MovedFileTracker,
        private val readerState: ReaderState = InMemoryReaderState(),
        private val sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>
    ) {
        private var delegateHolder = DelegateHolder<T>()
        private var onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit = { _, _ -> }
        private var onError: (StreamId?, String, Exception) -> Unit = { _, _, _ -> }
        private var sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR
        private val messageFilters: MutableSet<ReadMessageFilter> = hashSetOf()

        fun readFileImmediately(): Builder<T> = apply {
            canReadFile { _, _ -> true }
        }

        fun readFileAfterStaleTimeout(): Builder<T> = apply {
            canReadFileInternal(READ_AFTER_STALE_TIMEOUT)
        }

        fun canReadFile(condition: (Path, Duration) -> Boolean): Builder<T> = apply {
            canReadFileInternal { holder, stale -> condition(holder.path, stale) }
        }

        private fun canReadFileInternal(condition: (FileHolder<T>, Duration) -> Boolean): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(canRead = condition)
        }

        fun acceptNewerFiles(): Builder<T> = apply {
            acceptFiles(ACCEPT_NEWER_FILES)
        }

        fun acceptFiles(filter: (StreamId, Path?, Path) -> Boolean): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(acceptFile = filter)
        }

        fun onSourceFound(action: (StreamId, Path) -> Unit): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(onSourceFound = action)
        }

        fun onContentRead(action: (StreamId, Path, Collection<RawMessage.Builder>) -> Collection<RawMessage.Builder>): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(onContentRead = action)
        }

        fun onSourceCorrupted(action: (StreamId, Path, Exception) -> Unit): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(onSourceCorrupted = action)
        }

        fun onSourceClosed(action: (StreamId, Path) -> Unit): Builder<T> = apply {
            delegateHolder = delegateHolder.copy(onSourceClosed = action)
        }

        fun onStreamData(action: (StreamId, List<RawMessage.Builder>) -> Unit): Builder<T> = apply {
            onStreamData = action
        }

        fun onError(action: (StreamId?, String, Exception) -> Unit): Builder<T> = apply {
            onError = action
        }

        fun generateSequence(generator: (StreamId) -> Long): Builder<T> = apply {
            sequenceGenerator = generator
        }

        fun addMessageFilter(filter: ReadMessageFilter): Builder<T> = apply {
            messageFilters += filter
        }

        fun setMessageFilters(filters: Collection<ReadMessageFilter>): Builder<T> = apply {
            messageFilters.clear()
            messageFilters.addAll(filters)
        }

        fun build(): AbstractFileReader<T> {
            return DefaultFileReader(
                configuration,
                directoryChecker,
                contentParser,
                readerState,
                DelegateReaderListener(onStreamData, onError),
                delegateHolder,
                sequenceGenerator,
                sourceFactory,
                messageFilters,
            ).apply {
                init(fileTracker)
            }
        }
    }

    companion object {
        private val READ_AFTER_STALE_TIMEOUT: (FileHolder<*>, Duration) -> Boolean = { holder, stale ->
            abs(holder.lastModificationTime.toMillis() - Instant.now().toEpochMilli()) > stale.toMillis()
        }

        val ACCEPT_NEWER_FILES = { _: StreamId, current: Path?, new: Path ->
            current == null
                || Files.notExists(current)
                || current.attributes.creationTime() <= new.attributes.creationTime()
        }
    }
}


