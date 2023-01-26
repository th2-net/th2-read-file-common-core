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

import com.exactpro.th2.read.file.common.FileSourceWrapper
import java.io.InputStream

class StreamSourceWrapper<T : InputStream>(
    override val source: T
) : FileSourceWrapper<T> {
    init {
        check(source.markSupported()) { "The 'source' InputStream must support mark operation" }
    }
    override val hasMoreData: Boolean
        get() = source.available() > 0

    override fun mark(): Unit = source.mark(DEFAULT_READ_LIMIT)

    override fun reset(): Unit = source.reset()

    companion object {
        private const val DEFAULT_READ_LIMIT = 1024 * 1024
    }
}