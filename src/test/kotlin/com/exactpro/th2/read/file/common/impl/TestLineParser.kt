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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.read.file.common.AbstractFileTest
import com.exactpro.th2.read.file.common.StreamId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.lang.Thread.sleep
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.function.BiPredicate
import java.util.function.Function

internal class TestLineParser : AbstractFileTest() {
    @TempDir
    lateinit var dir: Path
    private val staleTimeout = Duration.ofSeconds(1)
    private val filter: BiPredicate<StreamId, String> = mock { onGeneric { test(any(), any()) }.thenReturn(true) }
    private val transformer: Function<String, String> = mock { onGeneric { apply(any()) }.thenAnswer { it.getArgument(0, String::class.java) } }
    private val parser = LineParser(filter, transformer)

    @Test
    internal fun `can parse file with more that one line`() {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", "line 2")
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isTrue()
        }
    }

    @Test
    internal fun `parse correct data from file`() {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", "line 2")
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)
            expectThat(parser.parse(streamId, sourceWrapper.source))
                .hasSize(1)
                .first()
                .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("line 1")
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    internal fun `can not parse file with only one line left at first time`(appendLf: Boolean) {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", lfInEnd = appendLf)
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isFalse()
        }
    }

    @Test
    internal fun `can parse file when new line added`() {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", lfInEnd = true)
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)

            sourceWrapper.mark()
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isFalse()
            sourceWrapper.reset()

            appendTo(dataFile, "line 2")

            sourceWrapper.mark()
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isTrue()
            sourceWrapper.reset()

            expectThat(parser.parse(streamId, sourceWrapper.source))
                .hasSize(1)
                .first()
                .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("line 1")
        }
    }

    @Test
    internal fun `can not parse file when last line changed`() {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", lfInEnd = false)
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)
            sourceWrapper.mark()
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isFalse()
            sourceWrapper.reset()

            appendTo(dataFile, "line 2")

            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isFalse()
        }
    }

    @Test
    internal fun `can parse file when last line does not change for stall timeout`() {
        val dataFile = createFile(dir, "data.txt")
        appendTo(dataFile, "line 1", lfInEnd = false)
        BufferedReaderSourceWrapper(Files.newBufferedReader(dataFile)).use { sourceWrapper ->
            val streamId = StreamId("test", Direction.FIRST)
            sourceWrapper.mark()
            expectThat(parser.canParse(streamId, sourceWrapper.source, false)).isFalse()
            sourceWrapper.reset()

            expect {
                sleep(staleTimeout.toMillis())

                sourceWrapper.mark()
                that(parser.canParse(streamId, sourceWrapper.source, true)).isTrue()
                sourceWrapper.reset()
                that(parser.parse(streamId, sourceWrapper.source))
                    .hasSize(1)
                    .first()
                    .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("line 1")
            }
        }
    }

    @Test
    internal fun `filters lines from the result`() {
        val file = createFile(dir, "data.txt").also {
            appendTo(it, "Line 1", "Line 2", "Line 3")
        }
        whenever(filter.test(any(), eq("Line 2"))).thenReturn(false)
        val streamId = StreamId("test", Direction.FIRST)

        Files.newBufferedReader(file).use {
            expect {
                that(parser.parse(streamId, it)).hasSize(1).first()
                    .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                that(parser.parse(streamId, it)).isEmpty()
                that(parser.parse(streamId, it)).hasSize(1).first()
                    .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 3")
            }
        }
    }

    @Test
    internal fun `transforms the accepted line`() {
        val file = createFile(dir, "data.txt").also {
            appendTo(it, "Line 1")
        }
        whenever(transformer.apply(eq("Line 1"))).thenReturn("Transformed line 1")
        val streamId = StreamId("test", Direction.FIRST)

        Files.newBufferedReader(file).use {
            expect {
                that(parser.parse(streamId, it)).hasSize(1).first()
                    .get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Transformed line 1")
            }
        }
    }
}