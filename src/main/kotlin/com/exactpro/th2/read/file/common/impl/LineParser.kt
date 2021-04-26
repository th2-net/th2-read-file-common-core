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
import com.exactpro.th2.read.file.common.ContentParser
import com.exactpro.th2.read.file.common.StreamId
import com.google.protobuf.ByteString
import java.io.BufferedReader
import java.util.function.BiPredicate
import java.util.function.Function

class LineParser @JvmOverloads constructor(
    private val filter: BiPredicate<StreamId, String> = BiPredicate { _, _ -> true },
    private val transformer: Function<String, String> = Function { it }
) : ContentParser<BufferedReader> {

    override fun canParse(streamId: StreamId, source: BufferedReader, considerNoFutureUpdates: Boolean): Boolean {
        val nextLine: String? = source.readLine()
        if (source.ready()) {
            return true
        }
        return nextLine != null && considerNoFutureUpdates
    }

    override fun parse(streamId: StreamId, source: BufferedReader): Collection<RawMessage.Builder> {
        val readLine = source.readLine()
        return if (readLine == null || !filter.test(streamId, readLine)) {
            emptyList()
        } else {
            listOf(RawMessage.newBuilder().setBody(ByteString.copyFrom(readLine.let(transformer::apply).toByteArray(Charsets.UTF_8))))
        }
    }

}