/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import io.prometheus.client.Histogram

object ReaderMetric {
    private val READER_PROCESSING_TIME: Histogram = Histogram.build(
        "th2_read_processing_time",
        "time in seconds read spent on particular action"
    ).labelNames("action").register()

    fun <T> measurePulling(action: () -> T): T = READER_PROCESSING_TIME.labels("pull").time(action)

    fun <T> measureReading(action: () -> T): T = READER_PROCESSING_TIME.labels("read").time(action)
}