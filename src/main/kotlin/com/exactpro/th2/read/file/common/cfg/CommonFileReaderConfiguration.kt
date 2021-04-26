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

package com.exactpro.th2.read.file.common.cfg

import java.time.Duration

class CommonFileReaderConfiguration(
    val staleTimeout: Duration = Duration.ofSeconds(5),
    val maxBatchSize: Int = 100,
    val maxPublicationDelay: Duration = Duration.ofSeconds(1),

    /**
     * Do not close the last file for stream ID until the new one is not found or the reader is not stopped
     */
    val leaveLastFileOpen: Boolean = true,
    /**
     * If it is enabled the incorrect timestamp (less than the previous one for the [com.exactpro.th2.read.file.common.StreamId])
     * will be fixed. Otherwise, the exception will be thrown and the source processing will be stopped
     */
    val fixTimestamp: Boolean = false,
) {
    init {
        check(staleTimeout.toMillis() > 0) { "'${::staleTimeout.name}' must be positive" }
        check(maxPublicationDelay.toMillis() >= 0) { "'${::maxPublicationDelay.name}' must be positive or zero" }
        check(maxBatchSize >= 0) { "'${::maxBatchSize.name}' must be positive or zero" }
    }
}
