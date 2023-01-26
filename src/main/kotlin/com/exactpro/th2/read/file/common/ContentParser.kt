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

package com.exactpro.th2.read.file.common

import com.exactpro.th2.common.grpc.RawMessage

interface ContentParser<in T> {

    /**
     * @param considerNoFutureUpdates if it is `true` the source was not changed for a timeout
     *          that might be considered as final file state
     * @return `true` if there is enough data in the source to parse it
     * @throws com.exactpro.th2.read.file.common.recovery.RecoverableException if the source needs to be reopen and recovered.
     * It is possible only if the source wrapper implements [com.exactpro.th2.read.file.common.recovery.RecoverableFileSourceWrapper]
     */
    fun canParse(streamId: StreamId, source: T, considerNoFutureUpdates: Boolean): Boolean

    /**
     * @return a collection of [RawMessage.Builder]s to be sent to the storage.
     *         If the collection is empty and more data can be parsed from the source
     *         another attempt to extract data will be performed
     */
    fun parse(streamId: StreamId, source: T): Collection<RawMessage.Builder>

}
