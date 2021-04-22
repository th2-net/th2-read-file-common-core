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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.LineParser
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyZeroInteractions
import strikt.api.expectThat
import strikt.assertions.get
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.io.BufferedReader
import java.nio.file.Path
import java.time.Duration

internal class TestAbstractFileReader : AbstractFileTest() {

    @TempDir
    lateinit var dir: Path

    private val staleTimeout = Duration.ofSeconds(1)
    private val parser: ContentParser<BufferedReader> = spy(LineParser())
    private val onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit = mock { }
    private val configuration = CommonFileReaderConfiguration(
        staleTimeout = staleTimeout,
        maxPublicationDelay = staleTimeout.multipliedBy(2),
        leaveLastFileOpen = true
    )

    private lateinit var reader: AbstractFileReader<BufferedReader>

    private lateinit var directoryChecker: DirectoryChecker

    @BeforeEach
    internal fun setUp() {
        directoryChecker = DirectoryChecker(
            dir,
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } },
            LAST_MODIFICATION_TIME_COMPARATOR
                .thenComparing { path -> path.nameParts().last().toInt() }
        )

        reader = TestLineReader(
            configuration,
            directoryChecker,
            parser,
            onStreamData
        )
    }

    @AfterEach
    internal fun tearDown() {
        reader.close()
    }

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
                get(0).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                get(1).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
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
                get(0).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                get(1).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
                get(2).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 3")
                get(3).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 4")
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
                get(0).get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 5")
            }
    }
}