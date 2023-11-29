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
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.FILE_NAME_PROPERTY
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.MESSAGE_STATUS_FIRST
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.MESSAGE_STATUS_LAST
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.MESSAGE_STATUS_PROPERTY
import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.MESSAGE_STATUS_SINGLE
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.get
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import java.time.Duration

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
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_FIRST)
                }
                get(1).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_LAST)
                }

                all { get { idBuilder() }.get { sessionAlias }.isEqualTo("A") }
            }
    }

    @Test
    internal fun `reads data as expected`() {
        createFile(dir, "A-0").also {
            appendTo(it, "Line 1", "Line 2", "Line 3")
        }

        createFile(dir, "A-1").also {
            appendTo(it, "Line")
        }

        val lastFile = createFile(dir, "A-2").also {
            appendTo(it, "Line 4", "Line 5")
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
            .hasSize(6)
            .apply {
                get(0).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 1")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_FIRST)
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-0")
                }
                get(1).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 2")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isNull()
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-0")
                }
                get(2).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 3")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_LAST)
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-0")
                }
                get(3).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_SINGLE)
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-1")
                }
                get(4).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 4")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_FIRST)
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-2")
                }
                get(5).run {
                    get { body }.get { toString(Charsets.UTF_8) }.isEqualTo("Line 5")
                    get { metadataBuilder() }.get { get(MESSAGE_STATUS_PROPERTY) }.isEqualTo(MESSAGE_STATUS_LAST)
                    get { metadataBuilder() }.get { get(FILE_NAME_PROPERTY) }.isEqualTo("A-2")
                }

                all { get { idBuilder() }.get { sessionAlias }.isEqualTo("A") }
            }
        clearInvocations(onStreamData)

        appendTo(lastFile, "Line 5")

        Thread.sleep(configuration.maxPublicationDelay.toMillis())
        assertTimeoutPreemptively(configuration.staleTimeout.plusMillis(200)) {
            reader.processUpdates()
        }

        verifyNoInteractions(onStreamData)
    }

    override fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration {
        return CommonFileReaderConfiguration(
            staleTimeout = defaultStaleTimeout,
            maxPublicationDelay = defaultStaleTimeout.multipliedBy(2),
            leaveLastFileOpen = false
        )
    }
}