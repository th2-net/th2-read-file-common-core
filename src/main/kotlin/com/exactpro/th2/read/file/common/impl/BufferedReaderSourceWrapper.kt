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

package com.exactpro.th2.read.file.common.impl

import com.exactpro.th2.read.file.common.FileSourceWrapper
import java.io.BufferedReader

/**
 * Wrapper for [BufferedReader] source. Can be used for sources that contains string data.
 *
 * **NOTE: this wrapper does not handle the half-written characters (for encoding with characters more than 1 byte length).**
 *
 * If you need to parse data with encoding that allows characters with more than 1 byte length please use [RecoverableBufferedReaderWrapper]
 */
open class BufferedReaderSourceWrapper<T : BufferedReader>(
    override val source: T
) : FileSourceWrapper<T> {
    override val hasMoreData: Boolean
        get() = try {
            mark();
            source.readLine() != null
        } finally {
            reset()
        }

    override fun mark(): Unit = source.mark(DEFAULT_READ_LIMIT)

    override fun reset(): Unit = source.reset()

    companion object {
        private const val DEFAULT_READ_LIMIT = 1024 * 1024
    }
}