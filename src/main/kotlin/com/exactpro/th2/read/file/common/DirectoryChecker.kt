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

import com.exactpro.th2.read.file.common.metric.FilesMetric
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.function.Consumer

class DirectoryChecker<K : DataGroupKey>(
    private val directory: Path,

    /**
     * Extracts the [DataGroupKey] from file name. If empty the file will be skipped
     */
    private val groupKeyExtractor: (Path) -> Set<K>,

    /**
     * Will be used to sort the files mapped to the same [DataGroupKey]
     */
    private val groupFileReorder: Consumer<MutableList<Path>>,
    private val filter: (Path) -> Boolean = { true }
) {
    constructor(
        directory: Path,
        groupFileReorder: Consumer<MutableList<Path>>,
        groupKeyExtractor: (Path) -> K?,
        filter: (Path) -> Boolean = { true }
    ) : this(directory, { path: Path -> groupKeyExtractor(path)?.let { setOf(it) } ?: emptySet() }, groupFileReorder, filter)

    constructor(
        directory: Path,
        groupFileComparator: Comparator<Path>,
        groupKeyExtractor: (Path) -> K?,
        filter: (Path) -> Boolean = { true }
    ) : this(directory, Consumer { it.sortWith(groupFileComparator) }, groupKeyExtractor, filter)

    /**
     * @return the list of files that are in the [directory]
     * and matches the [filter]
     */
    fun check(nextForGroup: (K, Path) -> Boolean = { _, _ -> true }): Map<K, Path> {
        if (Files.notExists(directory)) {
            LOGGER.warn { "Directory $directory does not exist. Skip checking" }
            return emptyMap()
        }
        val filesInDirectory: List<Path> = Files.newDirectoryStream(directory).use { dirStream ->
            dirStream.filter(::filterFile).toList()
        }

        FilesMetric.setFilesInDirectory(filesInDirectory.size)
        LOGGER.trace { "Collected ${filesInDirectory.size} file(s): $filesInDirectory" }

        val filesByGroupKey = hashMapOf<K, MutableList<Path>>()
        for (path in filesInDirectory) {
            val groups = groupKeyExtractor(path)
            if (groups.isEmpty()) {
                continue
            }
            groups.forEach { group ->
                filesByGroupKey.computeIfAbsent(group) { arrayListOf() }.add(path)
            }
        }

        return hashMapOf<K, Path>().also { dest ->
            filesByGroupKey.forEach { (group, files) ->
                files.apply {
                    groupFileReorder.accept(this)
                }.firstOrNull {
                    nextForGroup(group, it)
                }?.also { dest[group] = it }
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