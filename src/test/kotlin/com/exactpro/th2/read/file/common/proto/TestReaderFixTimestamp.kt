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
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.isEqualTo
import java.io.BufferedReader
import java.time.Duration
import java.time.Instant

class TestReaderFixTimestamp : AbstractReaderTest() {
    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = defaultStaleTimeout,
            maxPublicationDelay = Duration.ofSeconds(0),
            fixTimestamp = true,
        )
    }

    @Test
    internal fun `fixes timestamp`() {
        doReturn(true, false).whenever(parser).canParse(any(), any(), any())
        val now = Instant.now()
        val values = listOf(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.timestamp = now.toTimestamp()
                metadataBuilder.idBuilder.sequence = 1
            },
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.timestamp = now.minusSeconds(1).toTimestamp()
                metadataBuilder.idBuilder.sequence = 2
            }
        )
        doAnswer {
            val source = it.arguments[1] as BufferedReader
            source.readLine()
            return@doAnswer values
        }.whenever(parser).parse(any(), any())

        createFile(dir, "A-0").also {
            appendTo(it, "Line", lfInEnd = true)
        }
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        val expectedTimestamp = now.toTimestamp()
        val argumentCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), argumentCaptor.capture())

        expectThat(argumentCaptor.lastValue)
            .all {
                get { metadataBuilder }.get { id }.get { timestamp }.isEqualTo(expectedTimestamp)
            }
    }
}