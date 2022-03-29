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

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.MESSAGE_STATUS_PROPERTY
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.extensions.toTimestamp
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.never
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyZeroInteractions
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.allIndexed
import strikt.assertions.get
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.nio.file.Files
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class TestAbstractFileReader : AbstractReaderTest() {

    @Test
    internal fun `publishes all data on close`() {
        createFile(dir, "A-0").also {
            appendTo(it, "Line 1", "Line 2")
        }

        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        reader.close()

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), firstCaptor.capture())

        expectThat(firstCaptor.lastValue)
            .hasSize(2)
            .apply {
                get(0).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("START")
                }
                get(1).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("FIN")
                }

                all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
            }
    }

    @Test
    internal fun `reads data as expected`() {
        createFile(dir, "A-0").also {
            appendTo(it, "Line 1", "Line 2")
        }

        val lastFile = createFile(dir, "A-1").also {
            appendTo(it, "Line 3", "Line 4")
        }

        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        // Wait enough time to trigger publication
        Thread.sleep(configuration.maxPublicationDelay.toMillis())
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), firstCaptor.capture())

        expectThat(firstCaptor.allValues.flatten())
            .hasSize(4)
            .apply {
                get(0).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("START")
                }
                get(1).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("FIN")
                }
                get(2).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 3")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("START")
                }
                get(3).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 4")
                    get { metadata }.get { propertiesMap }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo("FIN")
                }

                all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
            }
        clearInvocations(onStreamData)

        appendTo(lastFile, "Line 5")

        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        verifyZeroInteractions(onStreamData)

        // Wait enough time to trigger publication
        Thread.sleep(configuration.maxPublicationDelay.toMillis())
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        val secondCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), secondCaptor.capture())
        expectThat(secondCaptor.allValues.flatten())
            .hasSize(1)
            .apply {
                get(0).apply {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 5")
                    get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A")
                }
            }
    }

    @Test
    @Disabled //FIXME
    internal fun `reads data from log rotation pattern`() {

        val exec = Executors.newSingleThreadScheduledExecutor()
        try {
            exec.scheduleWithFixedDelay(reader::processUpdates, 0, 1, TimeUnit.SECONDS)

            val logFile = createFile(dir, "log-0.log")
            val times = 20
            repeat(times) {
                appendTo(logFile, "log-$it", lfInEnd = true)
                Thread.sleep(10)
            }
            Files.move(logFile, logFile.resolveSibling("log-0.old"))
            Files.createFile(logFile)
            repeat(times) {
                appendTo(logFile, "log-${it + times}", lfInEnd = true)
                Thread.sleep(10)
            }

            val argumentCaptor = argumentCaptor<List<RawMessage.Builder>>()
            verify(onStreamData, timeout(configuration.maxPublicationDelay.plus(defaultStaleTimeout).toMillis()).times(1)).invoke(any(), argumentCaptor.capture())
            expectThat(argumentCaptor.allValues.flatten())
                .hasSize(times * 2)
                .apply {
                    allIndexed { index ->
                        get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("log-$index")
                    }
                    all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("log") }
                }
        } finally {
            exec.shutdown()
        }
    }

    @Test
    internal fun `stops reading data for stream when it fails the validation`() {
        doReturn(true).whenever(parser).canParse(any(), any(), any())
        val now = Instant.now()
        doReturn(
            listOf(
                RawMessage.newBuilder().apply { metadataBuilder.timestamp = now.toTimestamp() },
                RawMessage.newBuilder().apply { metadataBuilder.timestamp = now.minusSeconds(1).toTimestamp() }
            )
        ).whenever(parser).parse(any(), any())

        createFile(dir, "A-0").also {
            appendTo(it, "Line", lfInEnd = true)
        }
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }
        verify(parser).canParse(any(), any(), any())
        verify(parser).parse(any(), any())
        verify(onStreamData, never()).invoke(any(), any())

        clearInvocations(parser)

        createFile(dir, "A-1").also {
            appendTo(it, "line")
        }

        assertTimeoutPreemptively(configuration.staleTimeout) {
            reader.processUpdates()
        }

        verifyZeroInteractions(parser, onStreamData)
    }

    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = defaultStaleTimeout,
            maxPublicationDelay = defaultStaleTimeout.multipliedBy(2),
            leaveLastFileOpen = true,
        )
    }
}