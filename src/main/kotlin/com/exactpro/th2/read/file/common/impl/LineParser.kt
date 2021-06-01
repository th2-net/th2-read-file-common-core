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
import com.exactpro.th2.read.file.common.recovery.RecoverableException
import com.google.protobuf.ByteString
import java.io.BufferedReader
import java.nio.charset.Charset
import java.nio.charset.MalformedInputException
import java.util.function.BiPredicate
import java.util.function.Function

open class LineParser @JvmOverloads constructor(
    private val filter: BiPredicate<StreamId, String> = BiPredicate { _, _ -> true },
    private val transformer: Function<String, String> = Function { it }
) : ContentParser<BufferedReader> {

    override fun canParse(streamId: StreamId, source: BufferedReader, considerNoFutureUpdates: Boolean): Boolean {
        val nextLine: String? = try {
            source.readLine()
        } catch (ex: MalformedInputException) {
            if (considerNoFutureUpdates) {
                // because there won't be more bytes. so the file is corrupted
                throw ex
            }
            throw RecoverableException(ex)
        }
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
            lineToMessages(streamId, readLine.let(transformer::apply))
        }
    }

    protected open fun lineToMessages(streamId: StreamId, readLine: String): List<RawMessage.Builder> =
        listOf(lineToBuilder(readLine))

    @JvmOverloads
    protected fun lineToBuilder(readLine: String, charset: Charset = Charsets.UTF_8): RawMessage.Builder =
        RawMessage.newBuilder().setBody(ByteString.copyFrom(readLine.toByteArray(charset)))
}