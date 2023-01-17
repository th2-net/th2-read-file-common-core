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

import com.exactpro.th2.read.file.common.extensions.attributes
import com.sun.nio.file.SensitivityWatchEventModifier
import mu.KotlinLogging
import java.nio.file.FileSystems
import java.nio.file.FileVisitOption
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file.StandardWatchEventKinds.ENTRY_DELETE
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

/**
 * This class tries to track movements of files within the [dir].
 *
 * There is a [java.nio.file.attribute.BasicFileAttributes.fileKey] method that returns the key object that can identify the file.
 * But according to the method description it is not always possible. Because of that the movement tracking can fail in some cases.
 *
 * So, this class tries to provide the ability to check if two paths points to the same file. But there is no 100% guarantee that the result is correct.
 */
class MovedFileTracker @JvmOverloads constructor(
    private val dir: Path,
    forceNoKey: Boolean? = null,
) : AutoCloseable {
    private val watchService: WatchService = FileSystems.getDefault().newWatchService()
    private val listeners: MutableCollection<FileTrackerListener> = CopyOnWriteArrayList()
    private val fileTracker: FileTracker = collectInitialDirState(forceNoKey, listeners)

    init {
        check(Files.isDirectory(dir)) { "The $dir must be a directory" }
        dir.register(watchService, arrayOf(ENTRY_CREATE, ENTRY_DELETE), SensitivityWatchEventModifier.HIGH)
    }

    operator fun plusAssign(listener: FileTrackerListener) {
        listeners += listener
    }

    operator fun minusAssign(listener: FileTrackerListener) {
        listeners -= listener
    }

    @JvmOverloads
    @Throws(InterruptedException::class)
    fun pollFileSystemEvents(timeout: Long = 1, unit: TimeUnit = TimeUnit.SECONDS) {
        LOGGER.trace { "Polling events for $dir directory" }
        val timeToEnd: Long = System.currentTimeMillis() + unit.toMillis(timeout)
        val events = arrayListOf<WatchEvent<*>>()
        // Do so in the loop because sometimes the events might be split
        // and the first call of the `WatchService.poll` might return only part of the events
        // that we need to determinate whether the file was moved or deleted
        do {
            val watchKey: WatchKey? = watchService.poll(timeout, unit)
            if (watchKey == null) {
                LOGGER.trace { "No updates. Try again until timeout" }
                break
            }
            if (!watchKey.isValid) {
                LOGGER.error { "Watch key for $dir in not valid anymore" }
                break
            }
            events += watchKey.pollEvents()
            watchKey.reset()
        } while (timeToEnd > System.currentTimeMillis())

        events.forEach {
            val kind = it.kind()
            if (kind != ENTRY_DELETE && kind != ENTRY_CREATE) {
                return@forEach
            }
            val fullPath = (it.context() as Path).let(dir::resolve)

            LOGGER.trace { "Received event $kind for file $fullPath" }

            when (kind) {
                ENTRY_DELETE -> fileTracker.onDeleteEvent(fullPath)
                ENTRY_CREATE -> if (Files.exists(fullPath)) {
                    fileTracker.onCreateEvent(FileInfo(fullPath))
                }
                else -> LOGGER.warn { "Unsupported event kind: $kind" }
            }
        }
        fileTracker.applyPendingChanges()
    }

    /**
     * Tracks the movement of the files within the [dir] and returns `true`
     * if both paths point to the same actual file (the current and previous location, e.g).
     * Otherwise, returns ``false`.
     */
    fun isSameFiles(first: Path, second: Path): Boolean {
        val sameFiles = fileTracker.isSameFiles(first, second)
        LOGGER.trace { "Files $first and $second are ${if (sameFiles) "" else "not "}the same" }
        return sameFiles
    }

    override fun close() {
        watchService.close()
    }

    private fun collectInitialDirState(forceNoKey: Boolean?, listeners: Collection<FileTrackerListener>): FileTracker {
        val filesInDir = Files.walk(dir, 1, FileVisitOption.FOLLOW_LINKS)
            .filter { it != dir && Files.isRegularFile(it) }
            .map(::FileInfo)
            .collect(Collectors.toUnmodifiableList())

        return if (dir.attributes.fileKey() != null && forceNoKey != true) {
            // it is preferable option because it increases our chances to be right about file movement
            FileTracker.KeyFileTracker(filesInDir, listeners)
        } else {
            FileTracker.NoKeyFileTracker(filesInDir, listeners)
        }
    }

    interface FileTrackerListener {
        fun moved(prev: Path, current: Path)
        fun removed(paths: Set<Path>)
    }

    private class FileInfo(
        val path: Path
    ) {
        val key: Any?
        val size: Long
        val modificationTime: Instant

        init {
            with(path.attributes) {
                key = fileKey()
                size = size()
                modificationTime = lastModifiedTime().toInstant()
            }
        }
    }

    private sealed class FileTracker(private val listeners: Collection<FileTrackerListener>) {
        private val pendingDelete: MutableSet<Path> = ConcurrentHashMap.newKeySet()
        private val movedFiles: MutableMap<Path, FileInfo> = ConcurrentHashMap()

        fun onCreateEvent(fileInfo: FileInfo) {
            val movedFile = checkFileMoved(fileInfo, pendingDelete)
            if (movedFile == null) {
                onNewFile(fileInfo)
            } else {
                pendingDelete.remove(movedFile)
                // no need to mark the file as moved in case it was renamed to the original name eventually
                if (movedFile != fileInfo.path) {
                    movedFiles[movedFile] = fileInfo
                    listeners.forEach { it.moved(movedFile, fileInfo.path) }
                }
            }
        }

        fun onDeleteEvent(path: Path) {
            pendingDelete.add(path)
        }

        fun applyPendingChanges() {
            if (pendingDelete.isNotEmpty()) {
                filesRemoved(pendingDelete)
                listeners.forEach { it.removed(pendingDelete) }
                pendingDelete.clear()
            }
        }

        fun isSameFiles(first: Path, second: Path): Boolean {
            return isSameFiles(first, second, first)
        }

        protected open fun isSameFiles(first: Path, second: Path, stopFile: Path?): Boolean {
            val fileInfo = movedFiles[first]
            return when {
                fileInfo == null -> first == second
                fileInfo.path == second -> true
                fileInfo.path == stopFile -> false
                else -> isSameFiles(fileInfo.path, second, stopFile ?: fileInfo.path)
            }
        }

        abstract fun checkFileMoved(fileInfo: FileInfo, pendingDelete: Set<Path>): Path?
        protected abstract fun onNewFile(fileInfo: FileInfo)
        protected abstract fun filesRemoved(delete: MutableSet<Path>)

        class KeyFileTracker(
            filesInDir: List<FileInfo>,
            listeners: Collection<FileTrackerListener>
        ) : FileTracker(listeners) {
            private val keyToPath: MutableMap<Any, Path> = ConcurrentHashMap()
            private val pathToKey: MutableMap<Path, Any> = ConcurrentHashMap()

            init {
                filesInDir.forEach {
                    val key = it.keyRequire()
                    keyToPath[key] = it.path
                    pathToKey[it.path] = key
                }
            }

            override fun checkFileMoved(fileInfo: FileInfo, pendingDelete: Set<Path>): Path? {
                val currentAssociationWithKey = keyToPath[fileInfo.keyRequire()]
                return currentAssociationWithKey?.let { if (it in pendingDelete) it else null }
            }

            override fun onNewFile(fileInfo: FileInfo) {
                val key = fileInfo.keyRequire()
                keyToPath[key] = fileInfo.path
                pathToKey[fileInfo.path] = key
            }

            override fun filesRemoved(delete: MutableSet<Path>) {
                delete.forEach(pathToKey::remove)
                keyToPath.values.removeAll(delete)
            }

            override fun isSameFiles(first: Path, second: Path, stopFile: Path?): Boolean {
                if (Files.exists(first) && Files.exists(second)) {
                    return first.attributes.fileKey() == second.attributes.fileKey()
                }
                return super.isSameFiles(first, second, stopFile)
            }

            private fun FileInfo.keyRequire(): Any = checkNotNull(key) { "Key must be supported by the file system" }
        }

        class NoKeyFileTracker(filesInDir: List<FileInfo>, listeners: Collection<FileTrackerListener>) : FileTracker(listeners) {
            private val pathInfo: MutableMap<Path, FileInfo> = ConcurrentHashMap()

            init {
                filesInDir.associateByTo(pathInfo) { it.path }
            }

            override fun checkFileMoved(fileInfo: FileInfo, pendingDelete: Set<Path>): Path? {
                return pendingDelete.firstOrNull {
                    val knownInfo = pathInfo[it]
                    knownInfo != null
                        && knownInfo.modificationTime == fileInfo.modificationTime
                        && knownInfo.size == fileInfo.size
                }
            }

            override fun onNewFile(fileInfo: FileInfo) {
                pathInfo[fileInfo.path] = fileInfo
            }

            override fun filesRemoved(delete: MutableSet<Path>) {
                delete.forEach(pathInfo::remove)
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}