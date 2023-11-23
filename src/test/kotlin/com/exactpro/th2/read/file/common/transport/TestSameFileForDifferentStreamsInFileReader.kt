/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.read.file.common.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import strikt.api.expect
import strikt.assertions.all
import strikt.assertions.allIndexed
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.nio.file.Path
import java.time.Duration
import kotlin.system.measureTimeMillis

internal class TestSameFileForDifferentStreamsInFileReader : AbstractReaderTest() {

    @Test
    internal fun `reads data from the same file for different stream IDs`() {
        createFile(dir, "A-0").also {
            appendTo(it, "Line 0", "Line 1", "Line 2", "Line 3", "Line 4")
        }
        createFile(dir, "B-0").also {
            appendTo(it, "Line 0", "Line 1", "Line 2", "Line 3", "Line 4")
        }
        createFile(dir, "C-1").also {
            appendTo(it, "Line 0", "Line 1", "Line 2", "Line 3", "Line 4")
        }

        // The reader reads the whole file in one check because
        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            measureTimeMillis {
                reader.processUpdates()
            }
        }

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData, times(2)).invoke(any(), firstCaptor.capture())

        expect {
            val allBuilders = firstCaptor.allValues.flatten()
            that(allBuilders).hasSize(20)
            that(allBuilders.filter { it.idBuilder().sessionAlias == "A" })
                .hasSize(10)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it % 5}") }

                    all { get { idBuilder() }.get { sessionAlias }.isEqualTo("A") }
                }
            that(allBuilders.filter { it.idBuilder().sessionAlias == "B" })
                .hasSize(10)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it % 5}") }

                    all { get { idBuilder() }.get { sessionAlias }.isEqualTo("B") }
                }
        }
    }

    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = Duration.ofMillis(1),
            maxPublicationDelay = Duration.ofSeconds(2),
            maxBatchSize = 10,
            leaveLastFileOpen = false,
        )
    }

    override fun createExtractor(): (Path) -> Set<StreamId> {
        return { path ->
            val name = path.fileName.toString()
            hashSetOf<StreamId>().apply {
                if (name.contains('A') || name.contains('C')) {
                    add(StreamId("A"))
                }
                if (name.contains('B') || name.contains('C')) {
                    add(StreamId("B"))
                }
            }
        }
    }
}