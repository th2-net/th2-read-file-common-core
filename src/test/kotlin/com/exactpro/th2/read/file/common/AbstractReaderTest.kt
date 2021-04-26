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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.LineParser
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import java.io.BufferedReader
import java.nio.file.Path
import java.time.Duration

abstract class AbstractReaderTest : AbstractFileTest() {
    @TempDir
    lateinit var dir: Path
    protected val staleTimeout: Duration = Duration.ofSeconds(1)
    protected val parser: ContentParser<BufferedReader> = spy(LineParser())
    protected val onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit = mock { }
    protected lateinit var reader: AbstractFileReader<BufferedReader>
    protected lateinit var configuration: CommonFileReaderConfiguration
    private lateinit var directoryChecker: DirectoryChecker

    @BeforeEach
    internal fun setUp() {
        configuration = createConfiguration(staleTimeout)
        directoryChecker = DirectoryChecker(
            dir,
            { path -> path.nameParts().firstOrNull()?.let { StreamId(it, Direction.FIRST) } },
            { it.sortWith(LAST_MODIFICATION_TIME_COMPARATOR
                .thenComparing { path -> path.nameParts().last().toInt() }) }
        )

        val movedFileTracker = MovedFileTracker(dir)
        reader = TestLineReader(
            configuration,
            directoryChecker,
            parser,
            onStreamData,
        ).apply { init(movedFileTracker) }
    }

    abstract fun createConfiguration(staleTimeout: Duration): CommonFileReaderConfiguration

    @AfterEach
    internal fun tearDown() {
        reader.close()
    }
}