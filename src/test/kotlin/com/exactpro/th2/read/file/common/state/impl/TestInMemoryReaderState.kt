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
import com.exactpro.th2.read.file.common.state.StreamData
import com.google.protobuf.ByteString
import org.junit.jupiter.api.Test
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

internal class TestInMemoryReaderState {
    private val state = InMemoryReaderState()

    @ParameterizedTest(name = "IsProcessed: {0}")
    @ValueSource(booleans = [true, false])
    fun isFileProcessed(addToProcessed: Boolean) {
        val path = Path.of("test")
        val streamId = StreamId("test")
        if (addToProcessed) {
            state.fileProcessed(streamId, path)
        }

        expectThat(state.isFileProcessed(streamId, path)).isEqualTo(addToProcessed)
    }

    @Test
    fun processedFileMoved() {
        val path = Path.of("test")
        val current = Path.of("test_moved")
        val streamId = StreamId("test")
        state.fileProcessed(streamId, path)

        expect {
            that(state.isFileProcessed(streamId, path)).isTrue()
            that(state.fileMoved(path, current)).isTrue()
            that(state.isFileProcessed(streamId, path)).isFalse()
        }
    }

    @Test
    fun processedFilesRemoved() {
        val path = Path.of("test")
        val streamId = StreamId("test")
        state.fileProcessed(streamId, path)

        expect {
            that(state.isFileProcessed(streamId, path)).isTrue()
            state.processedFilesRemoved(listOf(path))
            that(state.isFileProcessed(streamId, path)).isFalse()
        }
    }

    @Test
    fun isStreamIdExcluded() {
        val streamId = StreamId("test")
        state.excludeStreamId(streamId)

        expect {
            that(state.isStreamIdExcluded(streamId)).isTrue()
        }
    }

    @Test
    fun `stores stream data`() {
        val streamId = StreamId("test")
        val data = StreamData(Instant.now(), 42, ByteString.copyFromUtf8("A"))

        expect {
            that(state[streamId]).isNull()
            state[streamId] = data
            that(state[streamId]).isSameInstanceAs(data)
        }
    }
}