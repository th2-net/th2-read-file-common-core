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

package com.exactpro.th2.read.file.common.proto

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import strikt.api.expect
import strikt.assertions.all
import strikt.assertions.allIndexed
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.time.Duration

internal class TestContinueOnFailureFileReader : AbstractReaderTest() {

    @Test
    fun `continues reading data for stream id`() {
        val errorFile = createFile(dir, "A-0")
        appendTo(errorFile, "Line 1", "Line 2", "Line 3", lfInEnd = true)

        doThrow(IllegalStateException("fake"))
            .doCallRealMethod()
            .whenever(parser).parse(any(), any())

        assertTimeoutPreemptively(Duration.ofSeconds(1).plusMillis(300)) {
            reader.processUpdates()
        }

        verifyNoInteractions(onStreamData)

        val newFile = createFile(dir, "A-1")
        writeTo(newFile,  "Line 4", "Line 5", "Line 6")

        // The reader reads the whole file in one check because of max batch size
        assertTimeoutPreemptively(Duration.ofSeconds(1).plusMillis(300)) {
            reader.processUpdates()
        }

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData, times(1)).invoke(any(), firstCaptor.capture())

        expect {
            that(firstCaptor.lastValue)
                .hasSize(configuration.maxBatchSize)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it + 4}") }

                    all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
                }
        }
    }

    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = Duration.ofMillis(1),
            maxPublicationDelay = Duration.ofSeconds(2),
            leaveLastFileOpen = true,
            maxBatchSize = 3,
            maxBatchesPerSecond = 1,
            allowFileTruncate = false,
            continueOnFailure = true,
        )
    }
}