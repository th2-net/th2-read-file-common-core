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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.toInstant
import com.exactpro.th2.read.file.common.AbstractFileReader
import com.exactpro.th2.read.file.common.ContentParser
import com.exactpro.th2.read.file.common.DirectoryChecker
import com.exactpro.th2.read.file.common.FileReaderHelper
import com.exactpro.th2.read.file.common.FileSourceWrapper
import com.exactpro.th2.read.file.common.MovedFileTracker
import com.exactpro.th2.read.file.common.ReadMessageFilter
import com.exactpro.th2.read.file.common.ReaderListener
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.attributes
import com.exactpro.th2.read.file.common.state.Content
import com.exactpro.th2.read.file.common.state.ProtoContent
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.StreamData
import com.exactpro.th2.read.file.common.state.TransportContent
import com.exactpro.th2.read.file.common.state.impl.InMemoryReaderState
import com.google.protobuf.TextFormat.shortDebugString
import org.apache.commons.lang3.builder.ToStringBuilder
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import kotlin.io.path.name
import kotlin.math.abs
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

abstract class DefaultFileReader<T : AutoCloseable, MESSAGE_BUILDER, ID_BUILDER> protected constructor(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker,
    contentParser: ContentParser<T, MESSAGE_BUILDER>,
    readerState: ReaderState,
    readerListener: ReaderListener<MESSAGE_BUILDER>,
    private val sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>,
    helper: FileReaderHelper<MESSAGE_BUILDER, ID_BUILDER>,
) : AbstractFileReader<T, MESSAGE_BUILDER, ID_BUILDER>(
    configuration,
    directoryChecker,
    contentParser,
    readerState,
    readerListener,
    helper,
) {
    private lateinit var delegateHolder: DelegateHolder<T, MESSAGE_BUILDER>

    override fun canReadRightNow(holder: FileHolder<T>, staleTimeout: Duration): Boolean = delegateHolder.canRead(holder, staleTimeout)
    override fun acceptFile(streamId: StreamId, currentFile: Path?, newFile: Path): Boolean = delegateHolder.acceptFile(streamId, currentFile, newFile)
    override fun createSource(streamId: StreamId, path: Path): FileSourceWrapper<T> = sourceFactory(streamId, path)

    override fun onSourceFound(streamId: StreamId, path: Path) {
        delegateHolder.onSourceFound.invoke(streamId, path)
    }

    override fun onContentRead(streamId: StreamId, path: Path, readContent: Collection<MESSAGE_BUILDER>): Collection<MESSAGE_BUILDER> {
        return delegateHolder.onContentRead.invoke(streamId, path, readContent)
    }

    override fun onSourceCorrupted(streamId: StreamId, path: Path, cause: Exception) {
        delegateHolder.onSourceCorrupted.invoke(streamId, path, cause)
    }

    override fun onSourceClosed(streamId: StreamId, path: Path) {
        delegateHolder.onSourceClosed.invoke(streamId, path)
    }

    private data class DelegateHolder<T : AutoCloseable, MESSAGE_BUILDER>(
        val canRead: (FileHolder<T>, Duration) -> Boolean = READ_AFTER_STALE_TIMEOUT,
        val acceptFile: (StreamId, Path?, Path) -> Boolean = { _, _, _ -> true },
        val onSourceFound: (StreamId, Path) -> Unit = { _, _ -> },
        val onContentRead: (StreamId, Path, Collection<MESSAGE_BUILDER>) -> Collection<MESSAGE_BUILDER> = { _, _, msgs -> msgs },
        val onSourceCorrupted: (StreamId, Path, Exception) -> Unit = { _, _, _ -> },
        val onSourceClosed: (StreamId, Path) -> Unit = { _, _ -> },
    )

    abstract class Builder<T : AutoCloseable, MESSAGE_BUILDER, MESSAGE_ID>(
        protected val configuration: CommonFileReaderConfiguration,
        protected val directoryChecker: DirectoryChecker,
        protected val contentParser: ContentParser<T, MESSAGE_BUILDER>,
        private val fileTracker: MovedFileTracker,
        protected val readerState: ReaderState = InMemoryReaderState(),
        protected val messageIdSupplier: (StreamId) -> MESSAGE_ID,
        protected val sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>
    ) {
        private var delegateHolder = DelegateHolder<T, MESSAGE_BUILDER>()
        protected var onStreamData: (StreamId, List<MESSAGE_BUILDER>) -> Unit = { _, _ -> }
        protected var onError: (StreamId?, String, Exception) -> Unit = { _, _, _ -> }
        protected var sequenceGenerator: (StreamId) -> Long = DEFAULT_SEQUENCE_GENERATOR
        protected val messageFilters: MutableSet<ReadMessageFilter<MESSAGE_BUILDER>> = hashSetOf()

        fun readFileImmediately(): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            canReadFile { _, _ -> true }
        }

        fun readFileAfterStaleTimeout(): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            canReadFileInternal(READ_AFTER_STALE_TIMEOUT)
        }

        fun canReadFile(condition: (Path, Duration) -> Boolean): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            canReadFileInternal { holder, stale -> condition(holder.path, stale) }
        }

        private fun canReadFileInternal(condition: (FileHolder<T>, Duration) -> Boolean): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(canRead = condition)
        }

        fun acceptNewerFiles(): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            acceptFiles(ACCEPT_NEWER_FILES)
        }

        fun acceptFiles(filter: (StreamId, Path?, Path) -> Boolean): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(acceptFile = filter)
        }

        fun onSourceFound(action: (StreamId, Path) -> Unit): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(onSourceFound = action)
        }

        fun onContentRead(action: (StreamId, Path, Collection<MESSAGE_BUILDER>) -> Collection<MESSAGE_BUILDER>): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(onContentRead = action)
        }

        fun onSourceCorrupted(action: (StreamId, Path, Exception) -> Unit): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(onSourceCorrupted = action)
        }

        fun onSourceClosed(action: (StreamId, Path) -> Unit): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            delegateHolder = delegateHolder.copy(onSourceClosed = action)
        }

        fun onStreamData(action: (StreamId, List<MESSAGE_BUILDER>) -> Unit): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            onStreamData = action
        }

        fun onError(action: (StreamId?, String, Exception) -> Unit): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            onError = action
        }

        fun generateSequence(generator: (StreamId) -> Long): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            sequenceGenerator = generator
        }

        fun addMessageFilter(filter: ReadMessageFilter<MESSAGE_BUILDER>): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            messageFilters += filter
        }

        fun setMessageFilters(filters: Collection<ReadMessageFilter<MESSAGE_BUILDER>>): Builder<T, MESSAGE_BUILDER, MESSAGE_ID> = apply {
            messageFilters.clear()
            messageFilters.addAll(filters)
        }

        fun build(): AbstractFileReader<T, MESSAGE_BUILDER, MESSAGE_ID> {
            val instance = createInstance()
            instance.delegateHolder = this.delegateHolder
            instance.init(fileTracker)
            return instance
        }

        protected abstract fun createInstance(): DefaultFileReader<T, MESSAGE_BUILDER, MESSAGE_ID>
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

class ProtoDefaultFileReader<T : AutoCloseable> private constructor(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker,
    contentParser: ContentParser<T, ProtoRawMessage.Builder>,
    readerState: ReaderState,
    readerListener: ReaderListener<ProtoRawMessage.Builder>,
    sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>,
    helper: FileReaderHelper<ProtoRawMessage.Builder, ProtoMessageID>,
) : DefaultFileReader<T, ProtoRawMessage.Builder, ProtoMessageID>(
    configuration,
    directoryChecker,
    contentParser,
    readerState,
    readerListener,
    sourceFactory,
    helper
) {
    override fun ProtoRawMessage.Builder.putMetadataProperty(key: String, value: String) {
        metadataBuilder.putProperties(key, value)
    }

    override fun setCommonInformation(
        fileHolder: FileHolder<T>,
        streamId: StreamId,
        readContent: Collection<ProtoRawMessage.Builder>,
        streamData: StreamData?
    ) {
        var sequence: Long = streamData?.run { lastSequence + 1 } ?: helper.generateSequence(streamId)
        readContent.forEach {
            it.metadataBuilder.apply {
                idBuilder.apply {
                    // set default parameters
                    mergeFrom(helper.createMessageId(streamId))
                    if (!hasTimestamp()) {
                        timestamp = Instant.now().toTimestamp()
                    }
                    connectionIdBuilder.sessionAlias = streamId.sessionAlias
                    setSequence(sequence++)
                }
                putProperties(FILE_NAME_PROPERTY, fileHolder.path.name)
            }
        }
    }

    override fun messageBuilderShortDebugString(builder: ProtoRawMessage.Builder): String = shortDebugString(builder)
    override fun ProtoRawMessage.Builder.toShortString(): String {
        val timestampOrNull =  if (metadata.id.hasTimestamp()) {
            metadata.id.timestamp.toInstant()
        } else {
            "none"
        }
        return "timestamp: $timestampOrNull; size: ${body.size()}"
    }

    override var ProtoRawMessage.Builder.messageTimestamp: Instant
        get() = metadata.id.timestamp.toInstant()
        set(value) { metadataBuilder.idBuilder.setTimestamp(value.toTimestamp()) }

    override val ProtoRawMessage.Builder.sequence: Long get() = metadata.id.sequence
    override val ProtoRawMessage.Builder.directionIsNoteSet: Boolean get() = metadataBuilder.idBuilder.direction == ProtoDirection.UNRECOGNIZED
    override val ProtoRawMessage.Builder.content: Content get() = ProtoContent(body)

    class Builder<T : AutoCloseable>(
        configuration: CommonFileReaderConfiguration,
        directoryChecker: DirectoryChecker,
        contentParser: ContentParser<T, ProtoRawMessage.Builder>,
        fileTracker: MovedFileTracker,
        readerState: ReaderState = InMemoryReaderState(),
        messageIdSupplier: (StreamId) -> ProtoMessageID,
        sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>
    ) : DefaultFileReader.Builder<T, ProtoRawMessage.Builder, ProtoMessageID>(
        configuration,
        directoryChecker,
        contentParser,
        fileTracker,
        readerState,
        messageIdSupplier,
        sourceFactory
    ) {
        override fun createInstance(): DefaultFileReader<T, ProtoRawMessage.Builder, ProtoMessageID> {
            return ProtoDefaultFileReader(
                configuration,
                directoryChecker,
                contentParser,
                readerState,
                DelegateReaderListener(onStreamData, onError),
                sourceFactory,
                ReaderHelper(
                    messageIdSupplier,
                    messageFilters,
                    sequenceGenerator,
                )
            )
        }
    }
}

class TransportDefaultFileReader<T : AutoCloseable> private constructor(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker,
    contentParser: ContentParser<T, RawMessage.Builder>,
    readerState: ReaderState,
    readerListener: ReaderListener<RawMessage.Builder>,
    sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>,
    helper: FileReaderHelper<RawMessage.Builder, MessageId.Builder>,
) : DefaultFileReader<T, RawMessage.Builder, MessageId.Builder>(
    configuration,
    directoryChecker,
    contentParser,
    readerState,
    readerListener,
    sourceFactory,
    helper
) {
    override fun RawMessage.Builder.putMetadataProperty(key: String, value: String) {
        metadataBuilder().put(key, value)
    }

    override fun setCommonInformation(
        fileHolder: FileHolder<T>,
        streamId: StreamId,
        readContent: Collection<RawMessage.Builder>,
        streamData: StreamData?
    ) {
        var sequence: Long = streamData?.run { lastSequence + 1 } ?: helper.generateSequence(streamId)
        readContent.forEach { builder ->
            val prototype = helper.createMessageId(streamId)

            builder.idBuilder().apply {
                // set default parameters
                if (prototype.isDirectionSet()) { setDirection(prototype.direction) }
                if (prototype.isTimestampSet()) { setTimestamp(prototype.timestamp) }
                // set default if empty parameter
                if (!isDirectionSet()) { setDirection(Direction.INCOMING) }
                if (!isTimestampSet()) { setTimestamp(Instant.now()) }
                // set actual values
                setSessionAlias(streamId.sessionAlias)
                setSequence(sequence++)
            }
            builder.putMetadataProperty(FILE_NAME_PROPERTY, fileHolder.path.name)
        }
    }

    override fun messageBuilderShortDebugString(builder: RawMessage.Builder): String = ToStringBuilder.reflectionToString(this)

    override fun RawMessage.Builder.toShortString(): String {
        val timestampOrNull: String? = try {
            idBuilder().timestamp.toString()
        } catch (e: IllegalStateException) {
            "none"
        }

        return "timestamp: $timestampOrNull; size: ${body.readableBytes()}"
    }

    override var RawMessage.Builder.messageTimestamp: Instant
        get() = idBuilder().timestamp
        set(value) { idBuilder().setTimestamp(value) }

    override val RawMessage.Builder.sequence: Long get() = this.idBuilder().sequence

    override val RawMessage.Builder.directionIsNoteSet: Boolean
        get() = !this.idBuilder().isDirectionSet()

    override val RawMessage.Builder.content: Content get() = TransportContent(body)

    class Builder<T : AutoCloseable>(
        configuration: CommonFileReaderConfiguration,
        directoryChecker: DirectoryChecker,
        contentParser: ContentParser<T, RawMessage.Builder>,
        fileTracker: MovedFileTracker,
        readerState: ReaderState = InMemoryReaderState(),
        messageIdSupplier: (StreamId) -> MessageId.Builder,
        sourceFactory: (StreamId, Path) -> FileSourceWrapper<T>
    ) : DefaultFileReader.Builder<T, RawMessage.Builder, MessageId.Builder>(
        configuration,
        directoryChecker,
        contentParser,
        fileTracker,
        readerState,
        messageIdSupplier,
        sourceFactory
    ) {
        override fun createInstance(): DefaultFileReader<T, RawMessage.Builder, MessageId.Builder> {
            return TransportDefaultFileReader(
                configuration,
                directoryChecker,
                contentParser,
                readerState,
                DelegateReaderListener(onStreamData, onError),
                sourceFactory,
                ReaderHelper(
                    messageIdSupplier,
                    messageFilters,
                    sequenceGenerator,
                )
            )
        }
    }
}