/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.BufferedReaderSourceWrapper
import com.exactpro.th2.read.file.common.impl.LineParser
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.allIndexed
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

class TestEndAwareFileSourceWrapper : AbstractReaderTest() {
    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            leaveLastFileOpen = true, // but the file will be closed if it marked as finished
            staleTimeout = Duration.ofSeconds(5),
        )
    }

    override fun createSource(id: StreamId, path: Path): FileSourceWrapper<BufferedReader> {
        return EndAwareBufferedReaderSource(EndAwareBufferedReader(path))
    }

    override fun createParser(): ContentParser<BufferedReader> = EndAwareLineParser()

    @Test
    fun `closes source before stale timeout expired`() {
        createFile(dir, "A-0").also {
            appendTo(it, "1", "2", "3", "end")
        }

        createFile(dir, "A-1").also {
            appendTo(it, "4", "5", "6", "end")
        }

        reader.use {
            assertTimeoutPreemptively(configuration.staleTimeout) {
                it.processUpdates()
            }
        }

        verify(onSourceClosed).invoke(any(), argThat { fileName.toString() == "A-0" })
        verify(onSourceClosed).invoke(any(), argThat { fileName.toString() == "A-1" })

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), firstCaptor.capture())

        expectThat(firstCaptor.lastValue)
            .hasSize(6)
            .apply {
                allIndexed { index ->
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("${index + 1}")
                }

                all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
            }
    }

    private class EndAwareLineParser : LineParser(
        filter = { _, line -> line != END_MARKER }
    ) {
        override fun canParse(streamId: StreamId, source: BufferedReader, considerNoFutureUpdates: Boolean): Boolean {
            val nextLine: String? = readNextPossibleLine(source, considerNoFutureUpdates)
            if (source.ready()) {
                return true
            }
            return nextLine != null && (considerNoFutureUpdates || nextLine == END_MARKER)
        }
    }

    private class EndAwareBufferedReaderSource(
        override val source: EndAwareBufferedReader
    ) : BufferedReaderSourceWrapper<EndAwareBufferedReader>(
        source
    ), EndAwareFileSourceWrapper<EndAwareBufferedReader> {
        override val fileEndReached: Boolean
            get() = source.finished
    }

    private class EndAwareBufferedReader(
        path: Path
    ) : BufferedReader(
        InputStreamReader(Files.newInputStream(path))
    ) {
        private var prevValue: Boolean? = null
        var finished: Boolean = false
            private set
        override fun readLine(): String? {
            return super.readLine()?.also {
                if (it == END_MARKER) {
                    finished = true
                }
            }
        }

        override fun mark(readAheadLimit: Int) {
            super.mark(readAheadLimit)
            prevValue = finished
        }

        override fun reset() {
            prevValue?.also { finished = it }
            prevValue = null
            super.reset()
        }
    }

    companion object {
        private const val END_MARKER = "end"
    }
}