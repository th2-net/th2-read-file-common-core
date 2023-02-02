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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.ContentParser
import com.exactpro.th2.read.file.common.DataGroupKey
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.read.file.common.recovery.RecoverableException
import com.google.protobuf.ByteString
import java.io.BufferedReader
import java.nio.charset.Charset
import java.nio.charset.MalformedInputException
import java.util.function.BiPredicate
import java.util.function.Function

open class LineParser<in K : DataGroupKey> @JvmOverloads constructor(
    private val extractStreamId: (K, String) -> StreamId,
    private val filter: BiPredicate<K, String> = BiPredicate { _, _ -> true },
    private val transformer: Function<String, String> = Function { it }
) : ContentParser<BufferedReader, K> {

    override fun canParse(dataGroup: K, source: BufferedReader, considerNoFutureUpdates: Boolean): Boolean {
        val nextLine: String? = readNextPossibleLine(source, considerNoFutureUpdates)
        if (source.ready()) {
            return true
        }
        return nextLine != null && considerNoFutureUpdates
    }

    override fun parse(dataGroup: K, source: BufferedReader): Map<StreamId, Collection<RawMessage.Builder>> {
        val readLine = source.readLine()
        return if (readLine == null || !filter.test(dataGroup, readLine)) {
            emptyMap()
        } else {
            lineToMessages(dataGroup, readLine.let(transformer::apply))
        }
    }

    protected fun readNextPossibleLine(source: BufferedReader, considerNoFutureUpdates: Boolean): String? = try {
        source.readLine()
    } catch (ex: MalformedInputException) {
        if (considerNoFutureUpdates) {
            // because there won't be more bytes. so the file is corrupted
            throw ex
        }
        throw RecoverableException(ex)
    }

    protected open fun lineToMessages(dataGroup: K, readLine: String): Map<StreamId, Collection<RawMessage.Builder>> {
        val builders = listOf(lineToBuilder(readLine))
        return if (builders.isNotEmpty()) {
            mapOf(extractStreamId(dataGroup, readLine) to builders)
        } else {
            emptyMap()
        }
    }

    @JvmOverloads
    protected fun lineToBuilder(readLine: String, charset: Charset = Charsets.UTF_8): RawMessage.Builder =
        RawMessage.newBuilder().setBody(ByteString.copyFrom(readLine.toByteArray(charset)))
}