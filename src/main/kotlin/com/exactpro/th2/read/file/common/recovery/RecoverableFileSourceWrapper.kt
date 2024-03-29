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

package com.exactpro.th2.read.file.common.recovery

import com.exactpro.th2.read.file.common.FileSourceWrapper

/**
 * The file source that implements that interface can be recovered
 * if it is required by the parser (the state of the source is not correct an it should be reopen
 * and recovered from the original source).
 *
 *
 */
interface RecoverableFileSourceWrapper<T : AutoCloseable> : FileSourceWrapper<T> {

    /**
     * Creates a new [RecoverableFileSourceWrapper] that contains the [source]
     * with the position that matches the position of the source holt by the current [RecoverableFileSourceWrapper]
     */
    fun recoverFrom(source: T): RecoverableFileSourceWrapper<T>
}