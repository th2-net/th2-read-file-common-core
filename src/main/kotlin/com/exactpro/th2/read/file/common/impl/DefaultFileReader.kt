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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.AbstractFileReader
import com.exactpro.th2.read.file.common.ContentParser
import com.exactpro.th2.read.file.common.DataGroupKey
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

class DefaultFileReader<T : AutoCloseable, K : DataGroupKey> private constructor(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker<K>,
    contentParser: ContentParser<T, K>,
    readerState: ReaderState<K>,
    readerListener: ReaderListener<K>,
    private val delegateHolder: DelegateHolder<T, K>,
    sequenceGenerator: (StreamId) -> Long,
    private val sourceFactory: (K, Path) -> FileSourceWrapper<T>,
    messageFilters: Collection<ReadMessageFilter>,
) : AbstractFileReader<T, K>(
    configuration,
    directoryChecker,
    contentParser,
    readerState,
    readerListener,
    sequenceGenerator,
    messageFilters,
) {
    override fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean = delegateHolder.canRead(holder, staleTimeout)

    override fun acceptFile(dataGroup: K, currentFile: Path?, newFile: Path): Boolean = delegateHolder.acceptFile(dataGroup, currentFile, newFile)

    override fun createSource(dataGroup: K, path: Path): FileSourceWrapper<T> = sourceFactory(dataGroup, path)

    override fun onSourceFound(dataGroup: K, path: Path) {
        delegateHolder.onSourceFound.invoke(dataGroup, path)
    }

    override fun onContentRead(dataGroup: K, streamId: StreamId, path: Path, readContent: Collection<RawMessage.Builder>): Collection<RawMessage.Builder> {
        return delegateHolder.onContentRead.invoke(streamId, path, readContent)
    }

    override fun onSourceCorrupted(dataGroup: K, path: Path, cause: Exception) {
        delegateHolder.onSourceCorrupted.invoke(dataGroup, path, cause)
    }

    override fun onSourceClosed(dataGroup: K, path: Path) {
        delegateHolder.onSourceClosed.invoke(dataGroup, path)
    }

    private data class DelegateHolder<T : AutoCloseable, K : DataGroupKey>(
        val canRead: (FileHolder<T>, Duration) -> Boolean = READ_AFTER_STALE_TIMEOUT,
        val acceptFile: (K, Path?, Path) -> Boolean = { _, _, _ -> true },
        val onSourceFound: (K, Path) -> Unit = { _, _ -> },
        val onContentRead: (StreamId, Path, Collection<RawMessage.Builder>) -> Collection<RawMessage.Builder> = { _, _, msgs -> msgs },
        val onSourceCorrupted: (K, Path, Exception) -> Unit = { _, _, _ -> },
        val onSourceClosed: (K, Path) -> Unit = { _, _ -> },
    )

    class Builder<T : AutoCloseable, K : DataGroupKey>(
        private val configuration: CommonFileReaderConfiguration,
        private val directoryChecker: DirectoryChecker<K>,
        private val contentParser: ContentParser<T, K>,
        private val fileTracker: MovedFileTracker,
        private val readerState: ReaderState<K> = InMemoryReaderState(),
        private val sourceFactory: (K, Path) -> FileSourceWrapper<T>
    ) {
        private var delegateHolder = DelegateHolder<T, K>()
        private var onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit = { _, _ -> }
        private var onError: (K?, String, Exception) -> Unit = { _, _, _ -> }
        private var sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR
        private val messageFilters: MutableSet<ReadMessageFilter> = hashSetOf()

        fun readFileImmediately(): Builder<T, K> = apply {
            canReadFile { _, _ -> true }
        }

        fun readFileAfterStaleTimeout(): Builder<T, K> = apply {
            canReadFileInternal(READ_AFTER_STALE_TIMEOUT)
        }

        fun canReadFile(condition: (Path, Duration) -> Boolean): Builder<T, K> = apply {
            canReadFileInternal { holder, stale -> condition(holder.path, stale) }
        }

        private fun canReadFileInternal(condition: (FileHolder<T>, Duration) -> Boolean): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(canRead = condition)
        }

        fun acceptNewerFiles(): Builder<T, K> = apply {
            acceptFiles(ACCEPT_NEWER_FILES)
        }

        fun acceptFiles(filter: (K, Path?, Path) -> Boolean): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(acceptFile = filter)
        }

        fun onSourceFound(action: (K, Path) -> Unit): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(onSourceFound = action)
        }

        fun onContentRead(action: (StreamId, Path, Collection<RawMessage.Builder>) -> Collection<RawMessage.Builder>): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(onContentRead = action)
        }

        fun onSourceCorrupted(action: (K, Path, Exception) -> Unit): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(onSourceCorrupted = action)
        }

        fun onSourceClosed(action: (K, Path) -> Unit): Builder<T, K> = apply {
            delegateHolder = delegateHolder.copy(onSourceClosed = action)
        }

        fun onStreamData(action: (StreamId, List<RawMessage.Builder>) -> Unit): Builder<T, K> = apply {
            onStreamData = action
        }

        fun onError(action: (K?, String, Exception) -> Unit): Builder<T, K> = apply {
            onError = action
        }

        fun generateSequence(generator: (StreamId) -> Long): Builder<T, K> = apply {
            sequenceGenerator = generator
        }

        fun addMessageFilter(filter: ReadMessageFilter): Builder<T, K> = apply {
            messageFilters += filter
        }

        fun setMessageFilters(filters: Collection<ReadMessageFilter>): Builder<T, K> = apply {
            messageFilters.clear()
            messageFilters.addAll(filters)
        }

        fun build(): AbstractFileReader<T, K> {
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

        private val ACCEPT_NEWER_FILES = { _: Any, current: Path?, new: Path ->
            current == null
                || Files.notExists(current)
                || current.attributes.creationTime() <= new.attributes.creationTime()
        }
    }
}


