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

@file:JvmName("ReaderHelper")
package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.read.file.common.AbstractFileReader
import com.exactpro.th2.read.file.common.FileReaderHelper
import com.exactpro.th2.read.file.common.ReadMessageFilter
import com.exactpro.th2.read.file.common.StreamId

@Suppress("FunctionName")
@JvmName("create")
@JvmOverloads
fun <MESSAGE_BUILDER, MESSAGE_ID> ReaderHelper(
    messageIdGenerator: (StreamId) -> MESSAGE_ID,
    messageFilters: Collection<ReadMessageFilter<MESSAGE_BUILDER>> = emptySet(),
    sequenceGenerator: (StreamId) -> Long = AbstractFileReader.DEFAULT_SEQUENCE_GENERATOR,
): FileReaderHelper<MESSAGE_BUILDER, MESSAGE_ID> {
    return ReaderHelperImpl(messageFilters, sequenceGenerator, messageIdGenerator)
}

private class ReaderHelperImpl<MESSAGE_BUILDER, MESSAGE_ID>(
    override val messageFilters: Collection<ReadMessageFilter<MESSAGE_BUILDER>>,
    private val sequenceGenerator: (StreamId) -> Long,
    private val messageIdGenerator: (StreamId) -> MESSAGE_ID,
) : FileReaderHelper<MESSAGE_BUILDER, MESSAGE_ID> {
    override fun generateSequence(streamId: StreamId): Long = sequenceGenerator(streamId)
    override fun createMessageId(streamId: StreamId): MESSAGE_ID = messageIdGenerator(streamId)
}