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

import com.exactpro.th2.common.grpc.Direction
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import strikt.api.expectThat
import strikt.assertions.hasEntry
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isNotEmpty
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.FileTime
import java.util.Comparator

internal class TestDirectoryChecker : AbstractFileTest() {

    @Test
    internal fun `retrieves first files`(@TempDir dest: Path) {
        createFiles(dest, "streamA-%d")
        createFiles(dest, "streamB-%d")

        val directoryChecker = DirectoryChecker(
            dest,
            LAST_MODIFICATION_TIME_COMPARATOR.thenComparing { path -> path.nameParts().last().toInt() },
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } }
        )
        expectThat(directoryChecker.check())
            .isNotEmpty()
            .hasSize(2)
            .hasEntry(StreamId("streamA", Direction.FIRST), dest.resolve("streamA-0"))
            .hasEntry(StreamId("streamB", Direction.FIRST), dest.resolve("streamB-0"))
    }

    @Test
    internal fun `retrieves files and applies pre-filter`(@TempDir dest: Path) {
        createFiles(dest, "streamA-%d")
        createFiles(dest, "streamB-%d")

        val directoryChecker = DirectoryChecker(
            dest,
            LAST_MODIFICATION_TIME_COMPARATOR.thenComparing { path -> path.nameParts().last().toInt() },
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } },
            { path -> path.fileName.toString().contains("streamA") }
        )
        expectThat(directoryChecker.check())
            .isNotEmpty()
            .hasSize(1)
            .hasEntry(StreamId("streamA", Direction.FIRST), dest.resolve("streamA-0"))
    }

    @Test
    internal fun `retrieves correct files if filter is passed`(@TempDir dest: Path) {
        createFiles(dest, "streamA-%d", count = 3)
        createFiles(dest, "streamB-%d", count = 3)

        val directoryChecker = DirectoryChecker(
            dest,
            LAST_MODIFICATION_TIME_COMPARATOR.thenComparing { path -> path.nameParts().last().toInt() },
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } }
        )

        val filterPath = { _: StreamId, path: Path -> path.nameParts().last().toInt() > 1 }

        expectThat(directoryChecker.check(filterPath))
            .isNotEmpty()
            .hasSize(2)
            .hasEntry(StreamId("streamA", Direction.FIRST), dest.resolve("streamA-2"))
            .hasEntry(StreamId("streamB", Direction.FIRST), dest.resolve("streamB-2"))
    }

    @Test
    internal fun `returns empty map if no files match the filter`(@TempDir dest: Path) {
        createFiles(dest, "streamA-%d", count = 2)
        createFiles(dest, "streamB-%d", count = 2)

        val directoryChecker = DirectoryChecker(
            dest,
            LAST_MODIFICATION_TIME_COMPARATOR.thenComparing { path -> path.nameParts().last().toInt() },
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } }
        )

        val filterPath = { _: StreamId, path: Path -> path.nameParts().last().toInt() > 1 }

        expectThat(directoryChecker.check(filterPath)).isEmpty()
    }

    @Throws(IOException::class)
    private fun createFiles(dir: Path, nameFormat: String, count: Int = 1, startIndex: Int = 0) {
        var index = startIndex
        repeat(count) {
            createFile(dir, nameFormat.format(index++))
        }
    }
}