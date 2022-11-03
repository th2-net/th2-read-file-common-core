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

package com.exactpro.th2.read.file.common.cfg

import com.exactpro.th2.read.file.common.AbstractFileReader.Companion.UNLIMITED_PUBLICATION
import java.time.Duration

class CommonFileReaderConfiguration @JvmOverloads constructor(
    /**
     * The timeout since the last file modification
     * before the reader starts to consider the source as finished (it won't be changes any more).
     * It allows the [com.exactpro.th2.read.file.common.ContentParser] to parse the data at the end of the file
     * because it won't change anymore.
     *
     * NOTE: if the file is actually changed after that,
     * the file is the last one for the [com.exactpro.th2.read.file.common.StreamId]
     * and [leaveLastFileOpen] is enabled the new data will be read
     */
    val staleTimeout: Duration = Duration.ofSeconds(5),

    /**
     * The maximum number of messages in a one batch.
     * If the reader tries to add a new portion of messages
     * to the batch and the new size is bigger than the [maxBatchSize]
     * the previous messages will be published before the new messages is added to the batch
     */
    val maxBatchSize: Int = 100,

    /**
     * The max delay the reader can delay the publication and accumulate the batch.
     *
     * NOTE: the reader published the batches only during update processing.
     * If the update processing method is not invoked the delayed batches won't be published
     * util the method is invoked. The actual delay might be bigger.
     */
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

    /**
     * Defines how many batches might be published in one second for a [com.exactpro.th2.read.file.common.StreamId].
     * If limit is reached the source reading will be suspended until the next second
     */
    val maxBatchesPerSecond: Int = UNLIMITED_PUBLICATION,
    /**
     * Disables tracking of files' movements.
     * The reader won't be able to determinate whether the already read file was moved or deleted and a new file with the same name was added a bit later.
     * But the reading will be a lot faster because the reader does not need to keep tracking updates from file system
     */
    val disableFileMovementTracking: Boolean = false,

    /**
     * If this setting is set to `true` the reader will reopen the file if it detects that it was truncated (the size is less than the original one).
     * If this setting is set to `false` the error will be reported for the StreamId and not more data will be read.
     */
    val allowFileTruncate: Boolean = false,

    /**
     * If enabled the reader will continue processing files for **StreamID** if an error was occurred when processing files for that stream.
     * The file that caused an error will be skipped and marked as processed.
     *
     * If disabled the reader will stop processing files for **StreamID** if any error was occurred.
     *
     * NOTE: this parameter might cause gaps in red data
     */
    val continueOnFailure: Boolean = false,

    /**
     * The min amount of time that must pass before the read will pull updates from the files system if it constantly read data.
     * This parameter is ignored if:
     * + reading from one of the streams has been finished
     */
    val minDelayBetweenUpdates: Duration = Duration.ZERO
) {
    init {
        check(staleTimeout.toMillis() >= 0) { "'${::staleTimeout.name}' must be positive or 0 (zero)" }
        check(maxPublicationDelay.toMillis() >= 0) { "'${::maxPublicationDelay.name}' must be positive or zero" }
        check(maxBatchSize >= 0) { "'${::maxBatchSize.name}' must be positive or zero" }
        check(maxBatchesPerSecond == UNLIMITED_PUBLICATION || maxBatchesPerSecond > 0) {
            "The publication limit must be ${::UNLIMITED_PUBLICATION.name}($UNLIMITED_PUBLICATION) or positive value"
        }
    }
}
