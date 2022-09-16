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

package com.exactpro.th2.read.file.common

import com.exactpro.th2.read.file.common.metric.FilesMetric
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.function.Consumer

class DirectoryChecker(
    private val directory: Path,

    /**
     * Extracts the [StreamId] from file name. If `null` the file will be skipped
     */
    private val streamIdExtractor: (Path) -> Set<StreamId>,

    /**
     * Will be used to sort the files mapped to the same [StreamId]
     */
    private val streamFileReorder: Consumer<MutableList<Path>>,
    private val filter: (Path) -> Boolean = { true }
) {
    constructor(
        directory: Path,
        streamFileReorder: Consumer<MutableList<Path>>,
        streamIdExtractor: (Path) -> StreamId?,
        filter: (Path) -> Boolean = { true }
    ) : this(directory, { path: Path -> streamIdExtractor(path)?.let { setOf(it) } ?: emptySet() }, streamFileReorder, filter)

    constructor(
        directory: Path,
        streamFileComparator: Comparator<Path>,
        streamIdExtractor: (Path) -> StreamId?,
        filter: (Path) -> Boolean = { true }
    ) : this(directory, Consumer { it.sortWith(streamFileComparator) }, streamIdExtractor, filter)

    /**
     * @return the list of files that are in the [directory]
     * and matches the [filter]
     */
    fun check(nextForStream: (StreamId, Path) -> Boolean = { _, _ -> true }): Map<StreamId, Path> {
        if (Files.notExists(directory)) {
            LOGGER.warn { "Directory $directory does not exist. Skip checking" }
            return emptyMap()
        }
        val filesInDirectory: List<Path> = Files.newDirectoryStream(directory).use { dirStream ->
            dirStream.filter(::filterFile).toList()
        }

        FilesMetric.setFilesInDirectory(filesInDirectory.size)
        LOGGER.trace { "Collected ${filesInDirectory.size} file(s): $filesInDirectory" }

        val filesByStreamId = hashMapOf<StreamId, MutableList<Path>>()
        for (path in filesInDirectory) {
            val streamIds = streamIdExtractor(path)
            if (streamIds.isEmpty()) {
                continue
            }
            streamIds.forEach { streamId ->
                filesByStreamId.computeIfAbsent(streamId) { arrayListOf() }.add(path)
            }
        }

        return hashMapOf<StreamId, Path>().also { dest ->
            filesByStreamId.forEach { (streamId, files) ->
                files.apply {
                    streamFileReorder.accept(this)
                }.firstOrNull {
                    nextForStream(streamId, it)
                }?.also { dest[streamId] = it }
            }
        }
    }

    private fun filterFile(path: Path): Boolean = try {
        Files.exists(path) && Files.isRegularFile(path) && filter.invoke(path)
    } catch (e: NoSuchFileException) {
        LOGGER.warn(e) { "File $path was removed when checking by filter" }
        false
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}