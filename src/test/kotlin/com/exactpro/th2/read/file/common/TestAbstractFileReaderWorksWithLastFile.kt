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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.allIndexed
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.time.Duration

internal class TestAbstractFileReaderWorksWithLastFile : AbstractReaderTest() {
    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = Duration.ofMillis(1),
            minDelayBetweenUpdates = Duration.ofSeconds(5),
            maxPublicationDelay = Duration.ofSeconds(2),
            leaveLastFileOpen = true,
            disableFileMovementTracking = true,
        )
    }

    @Test
    fun `does not close the last file`() {
        val file = createFile(dir, "A-0").apply {
            append("test1", "test2", lfInEnd = true)
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            reader.processUpdates()
        }

        with(file) {
            append("test3", lfInEnd = true)
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            reader.processUpdates()
        }

        reader.close()

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), firstCaptor.capture())
        expectThat(firstCaptor.allValues.flatten())
            .hasSize(3)
            .allIndexed {
                get { body.toStringUtf8() } isEqualTo "test${it + 1}"
            }
    }

    @Test
    fun `closes the last file if new one is found`() {
        createFile(dir, "A-0").apply {
            append("test1", "test2", lfInEnd = true)
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            reader.processUpdates()
        }

        createFile(dir, "A-1").apply {
            append("test3", lfInEnd = true)
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            reader.processUpdates()
        }

        reader.close()

        val firstCaptor = argumentCaptor<List<RawMessage.Builder>>()
        verify(onStreamData).invoke(any(), firstCaptor.capture())
        expectThat(firstCaptor.allValues.flatten())
            .hasSize(3)
            .allIndexed {
                get { body.toStringUtf8() } isEqualTo "test${it + 1}"
            }
    }
}