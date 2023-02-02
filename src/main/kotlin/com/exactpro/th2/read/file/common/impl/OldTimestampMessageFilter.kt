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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.RawMessageOrBuilder
import com.exactpro.th2.read.file.common.DataGroupKey
import com.exactpro.th2.read.file.common.FilterFileInfo
import com.exactpro.th2.read.file.common.ReadMessageFilter
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.extensions.toInstant
import com.exactpro.th2.read.file.common.state.GroupData
import com.exactpro.th2.read.file.common.state.StreamData
import java.time.Duration

/**
 * Filters messages that have timestamp less than the last timestamp from [StreamData]
 * and files that have last modification timestamp less than
 * the modification timestamp of the last read file for group ([GroupData])
 */
object OldTimestampMessageFilter : ReadMessageFilter {
    override fun drop(
        streamId: StreamId,
        message: RawMessageOrBuilder,
        streamData: StreamData?
    ): Boolean {
        if (streamData == null || !message.metadata.hasTimestamp()) {
            return false
        }
        val messageTimestamp = message.metadata.timestamp.toInstant()
        return streamData.lastTimestamp > messageTimestamp
            || (streamData.lastTimestamp == messageTimestamp && streamData.lastContent == message.body)
    }

    override fun drop(groupKey: DataGroupKey, fileInfo: FilterFileInfo, groupData: GroupData?): Boolean {
        if (groupData == null) {
            return false
        }
        return Duration.between(fileInfo.lastModified, groupData.lastSourceModificationTimestamp) > fileInfo.staleTimeout
    }
}