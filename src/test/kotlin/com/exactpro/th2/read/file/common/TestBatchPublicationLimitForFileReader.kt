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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.allIndexed
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo
import java.time.Duration
import kotlin.system.measureTimeMillis

internal class TestBatchPublicationLimitForFileReader : AbstractReaderTest() {

    @Test
    internal fun `publishes all data on close`() {
        createFile(dir, "A-0").also {
            appendTo(it, "Line 1", "Line 2", "Line 3", "Line 4", "Line 5", "Line 6")
        }

        // The reader reads the whole file in one check because
        val executionTime = assertTimeoutPreemptively(Duration.ofSeconds(1).plusMillis(300)) {
            measureTimeMillis {
                reader.processUpdates()
            }
        }

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData, times(3)).invoke(any(), firstCaptor.capture())

        expect {
            that(executionTime).isGreaterThanOrEqualTo(1_000) // we cannot publish all 3 batches less than in a second because of rate limit
            that(firstCaptor.firstValue)
                .hasSize(configuration.maxBatchSize)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it + 1}") }

                    all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
                }
            that(firstCaptor.secondValue)
                .hasSize(configuration.maxBatchSize)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it + 3}") }

                    all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
                }
            that(firstCaptor.thirdValue)
                .hasSize(configuration.maxBatchSize)
                .apply {
                    allIndexed { get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line ${it + 5}") }

                    all { get { metadata }.get { id }.get { connectionId }.get { sessionAlias }.isEqualTo("A") }
                }
        }
    }

    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = Duration.ofMillis(1),
            maxPublicationDelay = Duration.ofSeconds(2),
            leaveLastFileOpen = false,
            maxBatchSize = 2,
            maxBatchesPerSecond = 2,
        )
    }
}