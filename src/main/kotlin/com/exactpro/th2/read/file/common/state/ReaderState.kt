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

package com.exactpro.th2.read.file.common.state

import com.exactpro.th2.read.file.common.StreamId
import com.google.protobuf.ByteString
import io.netty.buffer.ByteBuf
import java.nio.file.Path
import java.time.Instant

interface ReaderState {
    fun isFileProcessed(streamId: StreamId, path: Path): Boolean
    fun fileProcessed(streamId: StreamId, path: Path)
    fun fileMoved(path: Path, current: Path): Boolean
    fun processedFilesRemoved(paths: Collection<Path>)

    fun isStreamIdExcluded(streamId: StreamId): Boolean
    fun excludeStreamId(streamId: StreamId)

    operator fun get(streamId: StreamId): StreamData?
    operator fun set(streamId: StreamId, data: StreamData)
}

sealed interface Content {
    fun isEqualTo(otherContent: Content): Boolean
}

class ProtoContent(
    private val content: ByteString
) : Content {
    override fun isEqualTo(otherContent: Content): Boolean = otherContent is ProtoContent && content == otherContent.content
}

class TransportContent(
    private val content: ByteBuf
) : Content {
    override fun isEqualTo(otherContent: Content): Boolean = otherContent is TransportContent && content == otherContent.content
}

data class StreamData(
    /**
     * Last timestamp used by the [StreamId]
     */
    val lastTimestamp: Instant,

    /**
     * Last sequence used by the [StreamId]
     */
    val lastSequence: Long,

    /**
     * The content of the last message in [StreamId]
     */
    val lastContent: Content
)