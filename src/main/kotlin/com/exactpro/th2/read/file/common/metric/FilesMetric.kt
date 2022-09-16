/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.read.file.common.metric

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

object FilesMetric {
    private val FILES_IN_DIRECTORY: Gauge = Gauge.build(
        "th2_read_files_in_dir_current",
        "current number of files in the directory that can be or was read by reader",
    ).register()
    private val PROCESSED_FILES_COUNTER: Counter = Counter.build(
        "th2_read_processed_files_count",
        "number of processed files by their status",
    ).labelNames("status").register()

    enum class ProcessStatus(val label: String) {
        /**
         * File was added to processing
         */
        FOUND("found"),

        /**
         * File was processed
         */
        PROCESSED("processed"),

        /**
         * File processing was terminated because of error
         */
        ERROR("error"),

        /**
         * File was excluded from processing because its stream was excluded (had an error before)
         */
        DROPPED("dropped")
    }

    fun setFilesInDirectory(count: Int) {
        FILES_IN_DIRECTORY.set(count.toDouble())
    }

    fun incStatus(status: ProcessStatus) {
        PROCESSED_FILES_COUNTER.labels(status.label).inc()
    }
}