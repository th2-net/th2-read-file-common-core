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

package com.exactpro.th2.read.file.common.state.impl

import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.StreamData
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

class InMemoryReaderState : ReaderState {
    private val processedFiles: MutableSet<Path> = ConcurrentHashMap.newKeySet()
    private val excludeStreamId: MutableSet<StreamId> = ConcurrentHashMap.newKeySet()
    private val streamDataByStreamId: MutableMap<StreamId, StreamData> = ConcurrentHashMap()

    override fun isFileProcessed(path: Path): Boolean = processedFiles.contains(path)

    override fun fileProcessed(path: Path) {
        processedFiles.add(path)
    }

    override fun processedFileRemoved(path: Path): Boolean = processedFiles.remove(path)

    override fun processedFilesRemoved(paths: Collection<Path>) {
        processedFiles.removeAll(paths)
    }

    override fun isStreamIdExcluded(streamId: StreamId): Boolean = excludeStreamId.contains(streamId)

    override fun excludeStreamId(streamId: StreamId) {
        excludeStreamId.add(streamId)
    }

    override fun get(streamId: StreamId): StreamData? = streamDataByStreamId[streamId]

    override fun set(streamId: StreamId, data: StreamData) {
        streamDataByStreamId[streamId] = data
    }
}