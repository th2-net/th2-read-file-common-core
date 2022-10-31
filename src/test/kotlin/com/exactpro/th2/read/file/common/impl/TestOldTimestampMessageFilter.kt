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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import com.exactpro.th2.read.file.common.state.StreamData
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.time.Instant

internal class TestOldTimestampMessageFilter {
    private val streamId = StreamId("test", Direction.FIRST)

    @Test
    fun `drops when timestamp in past`() {
        val timestamp = Instant.now()
        val builder = createBuilder(timestamp)
        val streamData = createStreamData(timestamp.plusSeconds(10))

        expectThat(OldTimestampMessageFilter.drop(streamId, builder, streamData)).isTrue()
    }

    @Test
    fun `drops when timestamp equals to last timestamp`() {
        val timestamp = Instant.now()
        val builder = createBuilder(timestamp)
        val streamData = createStreamData(timestamp)

        expectThat(OldTimestampMessageFilter.drop(streamId, builder, streamData)).isTrue()
    }

    @Test
    fun `does not drop when timestamp in future`() {
        val timestamp = Instant.now()
        val builder = createBuilder(timestamp)
        val streamData = createStreamData(timestamp.minusSeconds(10))

        expectThat(OldTimestampMessageFilter.drop(streamId, builder, streamData)).isFalse()
    }

    @Test
    fun `does not drop when timestamp is not set`() {
        val timestamp = Instant.now()
        val builder = createBuilder(null)
        val streamData = createStreamData(timestamp)

        expectThat(OldTimestampMessageFilter.drop(streamId, builder, streamData)).isFalse()
    }

    @Test
    fun `does not drop when stream data is null`() {
        val timestamp = Instant.now()
        val builder = createBuilder(timestamp)
        val streamData: StreamData? = null

        expectThat(OldTimestampMessageFilter.drop(streamId, builder, streamData)).isFalse()
    }

    private fun createBuilder(timestamp: Instant?) = RawMessage.newBuilder()
        .apply {
            if (timestamp == null) return@apply
            metadataBuilder.timestamp = timestamp.toTimestamp()
        }

    private fun createStreamData(timestamp: Instant) = StreamData(timestamp, -1)
}