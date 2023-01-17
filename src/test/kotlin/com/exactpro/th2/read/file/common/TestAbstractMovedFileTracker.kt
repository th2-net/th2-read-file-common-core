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

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

abstract class TestAbstractMovedFileTracker : AbstractFileTest() {
    @TempDir
    lateinit var dir: Path
    lateinit var tracker: MovedFileTracker

    @BeforeEach
    internal fun setUp() {
        tracker = createTracker(dir)
    }

    abstract fun createTracker(dir: Path): MovedFileTracker

    @Test
    fun `tracks file moving`() {
        val file = createFile(dir, "file1")
        tracker.pollFileSystemEvents(100, TimeUnit.MILLISECONDS)

        val movedFile = file.resolveSibling("file2")
        Files.move(file, movedFile)

        tracker.pollFileSystemEvents(100, TimeUnit.MILLISECONDS)

        expectThat(tracker.isSameFiles(file, movedFile)).isTrue()

    }

    @Test
    fun `circled rename`() {
        val file = createFile(dir, "file1")
        tracker.pollFileSystemEvents(100, TimeUnit.MILLISECONDS)

        val movedFile = file.resolveSibling("file2")
        Files.move(file, movedFile)
        Files.move(movedFile, file)

        tracker.pollFileSystemEvents(100, TimeUnit.MILLISECONDS)

        expectThat(tracker.isSameFiles(file, movedFile)).isFalse()
    }
}