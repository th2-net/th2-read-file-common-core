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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.state.StreamData
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNull
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isTrue
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.Path

internal class TestInMemoryReaderState {
    private val state = InMemoryReaderState()

    @ParameterizedTest(name = "IsProcessed: {0}")
    @ValueSource(booleans = [true, false])
    fun isFileProcessed(addToProcessed: Boolean) {
        val path = Path.of("test")
        if (addToProcessed) {
            state.fileProcessed(path)
        }

        expectThat(state.isFileProcessed(path)).isEqualTo(addToProcessed)
    }

    @Test
    fun processedFileRemoved() {
        val path = Path.of("test")
        state.fileProcessed(path)

        expect {
            that(state.isFileProcessed(path)).isTrue()
            that(state.processedFileRemoved(path)).isTrue()
            that(state.isFileProcessed(path)).isFalse()
        }
    }

    @Test
    fun processedFilesRemoved() {
        val path = Path.of("test")
        state.fileProcessed(path)

        expect {
            that(state.isFileProcessed(path)).isTrue()
            state.processedFilesRemoved(listOf(path))
            that(state.isFileProcessed(path)).isFalse()
        }
    }

    @Test
    fun isStreamIdExcluded() {
        val streamId = StreamId("test", Direction.FIRST)
        state.excludeStreamId(streamId)

        expect {
            that(state.isStreamIdExcluded(streamId)).isTrue()
            that(state.isStreamIdExcluded(StreamId("test", Direction.SECOND))).isFalse()
        }
    }

    @Test
    fun `stores stream data`() {
        val streamId = StreamId("test", Direction.SECOND)
        val data = StreamData(Instant.now(), 42)

        expect {
            that(state[streamId]).isNull()
            state[streamId] = data
            that(state[streamId]).isSameInstanceAs(data)
            that(state[StreamId("test", Direction.FIRST)]).isNull()
        }
    }
}