/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.exactpro.th2.read.file.common.state.StreamData
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

interface ReadMessageFilter {
    /**
     * Returns `true` if the message should be dropped from publication
     * @param streamId stream identificator
     * @param message message read from the source
     * @param streamData state for the current [streamId]
     */
    fun drop(
        streamId: StreamId,
        message: RawMessageOrBuilder,
        streamData: StreamData?
    ): Boolean

    /**
     * Returns `true` if the whole file should be dropped from processing
     * @param streamId stream identificator
     * @param fileInfo information about current file
     * @param streamData state for the current [streamId]
     */
    fun drop(
        streamId: StreamId,
        fileInfo: FilterFileInfo,
        streamData: StreamData?,
    ): Boolean
}

data class FilterFileInfo(
    val path: Path,
    val lastModified: Instant,
    val staleTimeout: Duration,
)
