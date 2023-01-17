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

package com.exactpro.th2.read.file.common.state.impl

import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.StreamData
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class InMemoryReaderState : ReaderState {
    private val lock = ReentrantReadWriteLock()
    private val processedFiles: MutableMap<StreamId, MutableSet<Path>> = HashMap()
    private val excludeStreamId: MutableSet<StreamId> = ConcurrentHashMap.newKeySet()
    private val streamDataByStreamId: MutableMap<StreamId, StreamData> = ConcurrentHashMap()

    override fun isFileProcessed(streamId: StreamId, path: Path): Boolean {
        return lock.read { processedFiles[streamId]?.contains(path) ?: false }
    }

    override fun fileProcessed(streamId: StreamId, path: Path) {
        lock.write { processedFiles.computeIfAbsent(streamId) { HashSet() }.add(path) }
    }

    override fun fileMoved(path: Path, current: Path): Boolean {
        var found = false
        lock.write {
            processedFiles.forEach { (_, files) ->
                if (files.remove(path)) {
                    files.add(current)
                    found = true
                }
            }
        }
        return found
    }

    override fun processedFilesRemoved(paths: Collection<Path>) {
        lock.write { processedFiles.values.forEach { it.removeAll(paths) } }
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