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

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import com.exactpro.th2.read.file.common.impl.OldTimestampMessageFilter
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.get
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.io.BufferedReader
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

internal class TestReaderDropsOldTimestamp : AbstractReaderTest() {
    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = defaultStaleTimeout,
            maxPublicationDelay = Duration.ofSeconds(1),
        )
    }

    override val messageFilters: Collection<ReadMessageFilter>
        get() = listOf(OldTimestampMessageFilter)

    @Test
    fun `applies old timestamp filter`() {
        doReturn(true, true, true, false).whenever(parser).canParse(any(), any(), any())
        val now = Instant.now()
        val values = listOf(
            RawMessage.newBuilder().apply { metadataBuilder.timestamp = now.toTimestamp() },
            RawMessage.newBuilder().apply { metadataBuilder.timestamp = now.minusSeconds(1).toTimestamp() },
            RawMessage.newBuilder().apply { metadataBuilder.timestamp = now.plusNanos(1).toTimestamp() },
        )
        var answerIndex = 0
        doAnswer {
            val source = it.arguments[1] as BufferedReader
            source.readLine()
            return@doAnswer listOf(values[answerIndex++])
        }.whenever(parser).parse(any(), any())

        createFile(dir, "A-0").also {
            appendTo(it, "Line1", lfInEnd = true)
            appendTo(it, "Line2", lfInEnd = true)
            appendTo(it, "Line3", lfInEnd = true)
        }
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }
        Thread.sleep(1000)
        assertTimeoutPreemptively(Duration.ofMillis(200)) {
            reader.processUpdates()
        }

        val argumentCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), argumentCaptor.capture())
        expectThat(argumentCaptor.lastValue)
            .hasSize(2)
            .apply {
                get(0).get { metadataBuilder }.get { timestamp }.isEqualTo(now.toTimestamp())
                get(1).get { metadataBuilder }.get { timestamp }.isEqualTo(now.plusNanos(1).toTimestamp())
            }
    }
}