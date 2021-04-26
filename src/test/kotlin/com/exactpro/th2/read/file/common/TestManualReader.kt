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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.BufferedReaderSourceWrapper
import com.exactpro.th2.read.file.common.impl.DefaultFileReader
import com.exactpro.th2.read.file.common.impl.LineParser
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.io.path.ExperimentalPathApi
import kotlin.random.Random

@Disabled
@ExperimentalPathApi
class TestManualReader : AbstractFileTest() {
    private val dir: Path = Path.of("build/workdir")
    private val idExtractor: (Path) -> StreamId? = { path ->
        path.nameParts().let {
            if (it.size == 2) {
                StreamId(it.first(), Direction.FIRST)
            } else {
                null
            }
        }
    }
    private val checker = DirectoryChecker(
        dir,
        idExtractor,
        { it.sortWith(LAST_MODIFICATION_TIME_COMPARATOR
            .thenComparing { path -> path.nameParts()[1].split('.', limit = 2)[0].toInt() }) }
    )

    private lateinit var reader: AbstractFileReader<BufferedReader>
    private lateinit var executor: ScheduledExecutorService
    private lateinit var future: Future<*>

    @BeforeEach
    internal fun setUp() {
        val configuration = CommonFileReaderConfiguration(
            staleTimeout = Duration.ofSeconds(2),
            maxPublicationDelay = Duration.ofSeconds(2),
            leaveLastFileOpen = true,
        )
        dir.toFile().deleteRecursively()
        Files.createDirectory(dir)

        val movedFileTracker = MovedFileTracker(dir)
        reader = DefaultFileReader.Builder(
            configuration,
            checker,
            LineParser(),
            movedFileTracker,
        ) { _, path -> BufferedReaderSourceWrapper(Files.newBufferedReader(path)) }
            .readFileImmediately()
            .acceptNewerFiles()
            .onStreamData { streamId, list ->
                LOGGER.info { "Published: streamID: $streamId; data: ${list.joinToString { shortDebugString(it) }}" }
            }.build()
        executor = Executors.newSingleThreadScheduledExecutor()

        future = executor.scheduleWithFixedDelay(reader::processUpdates, 0, 5, TimeUnit.SECONDS)
    }

    @AfterEach
    internal fun tearDown() {
        future.cancel(false)
        executor.shutdown()
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOGGER.warn { "Executor was not shutdown" }
            executor.shutdownNow()
        }
        reader.close()
    }

    @Test
    fun manual() {
        while (!Thread.currentThread().isInterrupted) {
            Thread.sleep(1000)
        }
    }

    @Test
    fun `manual generation`() {
        val aliases = setOf("A", "B", "C", "D")

        class Data(
            private val alias: String
        ) {
            var index: Int = 0
            var linesWrite: Int = 0
            val fileName: String
                get() = "$alias-%d.txt".format(index)

            fun lineAdded(limit: Int) {
                linesWrite++
                if (linesWrite >= limit) {
                    index++
                    linesWrite = 0
                }
            }
        }

        val dataByAlias = hashMapOf<String, Data>()
        val linePerFileLimit = 100

        val random = Random(System.currentTimeMillis())
        repeat(500) {
            aliases.forEach { alias ->
                val data = dataByAlias.computeIfAbsent(alias, ::Data)
                val file = dir.resolve(data.fileName)
                if (Files.notExists(file)) {
                    Files.createFile(file)
                }
                appendTo(file, RandomStringUtils.randomAlphabetic(50, 100), lfInEnd = true)
                data.lineAdded(linePerFileLimit)
            }
            Thread.sleep(random.nextLong(10, 100))
        }
    }

    @Test
    fun `manual log generation`() {
        val logFile = createFile(dir, "log-0.log")
        val random = Random(System.currentTimeMillis())
        var copy: Int = 0
        var linesInFile = 0
        val linesPerFile = 100
        repeat(1000) {
            linesInFile++
            appendTo(logFile, "log-line-$it:${RandomStringUtils.randomAlphabetic(50, 100)}", lfInEnd = true)
            if (linesInFile >= linesPerFile) {
                linesInFile = 0
                Files.move(logFile, logFile.resolveSibling("log-0.log.${copy++}"))
                Files.createFile(logFile)
            }
            Thread.sleep(random.nextLong(10, 100))
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}