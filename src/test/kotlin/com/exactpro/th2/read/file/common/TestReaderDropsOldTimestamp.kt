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

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import com.exactpro.th2.read.file.common.impl.LineParser
import com.exactpro.th2.read.file.common.impl.OldTimestampMessageFilter
import com.exactpro.th2.read.file.common.state.StreamData
import com.google.protobuf.ByteString
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
import strikt.assertions.single
import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.time.Duration
import java.time.Instant

internal class TestReaderDropsOldTimestamp : AbstractReaderTest() {
    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = Duration.ofMillis(10),
            maxPublicationDelay = Duration.ofSeconds(1),
            leaveLastFileOpen = true,
        )
    }

    override val messageFilters: Collection<ReadMessageFilter>
        get() = listOf(OldTimestampMessageFilter)

    @Test
    fun `applies old timestamp filter`() {
        val start = Instant.now()
        val first: Instant
        val last: Instant
        createFile(dir, "A-0").also {
            first = start.minusSeconds(2)
            appendTo(it, "$first", lfInEnd = true)
            appendTo(it, "${start.minusSeconds(10)}", lfInEnd = true)
            last = start.minusSeconds(1)
            appendTo(it, "$last", lfInEnd = true)
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
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
                get(0).get { metadataBuilder }.get { id }.get { timestamp }.isEqualTo(first.toTimestamp())
                get(1).get { metadataBuilder }.get { id }.get { timestamp }.isEqualTo(last.toTimestamp())
            }
    }

    @Test
    fun `drops files with old last modification time`() {
        createFile(dir, "A-0").apply {
            append("${Instant.now()}", lfInEnd = true)
        }
        Thread.sleep(configuration.staleTimeout.toMillis() + 10)
        val creationTime = Instant.now()
        createFile(dir, "A-1").apply {
            append("$creationTime", lfInEnd = true)
        }
        readerState[StreamId("A", Direction.FIRST)] = StreamData(creationTime.minusMillis(1), -1, ByteString.EMPTY)

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            reader.processUpdates()
        }
        Thread.sleep(1000)
        assertTimeoutPreemptively(Duration.ofMillis(200)) {
            reader.processUpdates()
        }
        val argumentCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), argumentCaptor.capture())
        expectThat(argumentCaptor.lastValue)
            .single().get { metadataBuilder }.get { id }.get { timestamp }.isEqualTo(creationTime.toTimestamp())
    }

    override fun createParser(): ContentParser<BufferedReader> {
        return object : LineParser() {
            override fun parse(streamId: StreamId, source: BufferedReader): Collection<RawMessage.Builder> {
                return super.parse(streamId, source).onEach {
                    val data = it.body.toStringUtf8()
                    it.metadataBuilder.idBuilder.timestamp = Instant.parse(data).toTimestamp()
                }
            }
        }
    }
}