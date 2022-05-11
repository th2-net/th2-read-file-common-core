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

import com.exactpro.th2.read.file.common.recovery.RecoverableFileSourceWrapper
import java.io.LineNumberReader

/**
 * This implementation of [java.io.BufferedReader] wrapper allows the source to be recovered
 * if the parser requires source to be reopen because some exception has been thrown
 */
class RecoverableBufferedReaderWrapper(
    source: LineNumberReader
) : BufferedReaderSourceWrapper<LineNumberReader>(source), RecoverableFileSourceWrapper<LineNumberReader> {

    override fun recoverFrom(source: LineNumberReader): RecoverableFileSourceWrapper<LineNumberReader> {
        val lineNumber = this.source.lineNumber
        repeat(lineNumber) { source.readLine() }
        return RecoverableBufferedReaderWrapper(source)
    }
}