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

package com.exactpro.th2.read.file.common

import mu.KotlinLogging
import java.nio.file.FileVisitOption
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class DirectoryChecker(
    private val directory: Path,

    /**
     * Extracts the [StreamId] from file name. If `null` the file will be skipped
     */
    private val streamIdExtractor: (Path) -> StreamId?,
    /**
     * Will be used to sort the files mapped to the same [StreamId]
     */
    private val streamFileComparator: Comparator<Path>,
    private val filter: (Path) -> Boolean = { true }
) {
    /**
     * @return the list of files that are in the [directory]
     * and matches the [filter]
     */
    fun check(nextForStream: (StreamId, Path) -> Boolean = { _, _ -> true }): Map<StreamId, Path> {
        if (Files.notExists(directory)) {
            LOGGER.warn { "Directory $directory does not exist. Skip checking" }
            return emptyMap()
        }
        val filesInDirectory = Files.walk(directory, 1, FileVisitOption.FOLLOW_LINKS)
            .filter { Files.isRegularFile(it) && filter.invoke(it) }
            .collect(Collectors.toUnmodifiableList())

        LOGGER.trace { "Collected ${filesInDirectory.size} file(s): $filesInDirectory" }

        val filesByStreamId = hashMapOf<StreamId, MutableList<Path>>()
        for (path in filesInDirectory) {
            val streamId = streamIdExtractor(path) ?: continue
            filesByStreamId.computeIfAbsent(streamId) { arrayListOf() }.add(path)
        }

        return hashMapOf<StreamId, Path>().also { dest ->
            filesByStreamId.forEach { (streamId, files) ->
                files.apply {
                    sortWith(streamFileComparator)
                }.firstOrNull {
                    nextForStream(streamId, it)
                }?.also { dest[streamId] = it }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}