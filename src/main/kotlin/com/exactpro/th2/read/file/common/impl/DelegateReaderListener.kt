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

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.ReaderListener
import com.exactpro.th2.read.file.common.StreamId

class DelegateReaderListener(
    private val onStreamDataDelegate: (StreamId, List<RawMessage.Builder>) -> Unit = { _, _ -> },
    private val onErrorDelegate: (StreamId?, String, Exception) -> Unit = { _, _, _ -> },
) : ReaderListener {
    override fun onStreamData(streamId: StreamId, messages: List<RawMessage.Builder>) {
        onStreamDataDelegate(streamId, messages)
    }

    override fun onError(streamId: StreamId?, message: String, cause: Exception) {
        onErrorDelegate(streamId, message, cause)
    }
}