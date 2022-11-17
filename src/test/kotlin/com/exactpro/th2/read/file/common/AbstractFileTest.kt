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

import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributeView

open class AbstractFileTest {
    protected fun createFile(dir: Path, fileName: String): Path {
        val filePath = dir.resolve(fileName)
        Files.deleteIfExists(filePath)
        LOGGER.debug { "Creating file $filePath" }
        return Files.createFile(filePath)
    }

    protected fun appendTo(file: Path, vararg lines: String, lfInEnd: Boolean = false) {
        write(file, lines, lfInEnd, StandardOpenOption.APPEND)
    }

    protected fun writeTo(file: Path, vararg lines: String, lfInEnd: Boolean = false) {
        write(file, lines, lfInEnd)
    }

    protected fun appendTo(file: Path, bytes: ByteArray) {
        if (bytes.isEmpty()) {
            return
        }
        Files.newOutputStream(file, StandardOpenOption.APPEND).use {
            it.write(bytes)
        }
    }

    protected fun Path.append(vararg lines: String, lfInEnd: Boolean = false) {
        appendTo(this, *lines, lfInEnd = lfInEnd)
    }

    protected fun Path.append(bytes: ByteArray) {
        appendTo(this, bytes)
    }

    protected fun Path.write(vararg lines: String, lfInEnd: Boolean = false) {
        writeTo(this, *lines, lfInEnd = lfInEnd)
    }

    private fun write(file: Path, lines: Array<out String>, lfInEnd: Boolean, vararg options: StandardOpenOption) {
        Files.newBufferedWriter(file, *options).use {
            for ((index, line) in lines.withIndex()) {
                it.append(line)
                if (index == lines.size - 1 && !lfInEnd) {
                    continue
                }
                it.newLine()
            }
        }
    }

    protected companion object {
        private val LOGGER = KotlinLogging.logger { }
        val LAST_MODIFICATION_TIME_COMPARATOR: Comparator<Path> = Comparator.comparing {
            Files.getFileAttributeView(it, BasicFileAttributeView::class.java).readAttributes().lastModifiedTime()
        }

        fun Path.nameParts() = fileName.toString().split('-')
    }
}