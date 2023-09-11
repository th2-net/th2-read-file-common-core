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

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.BufferedReaderSourceWrapper
import com.exactpro.th2.read.file.common.impl.ProtoDefaultFileReader
import com.exactpro.th2.read.file.common.impl.LineParser
import com.exactpro.th2.read.file.common.state.ReaderState
import com.exactpro.th2.read.file.common.state.impl.InMemoryReaderState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

abstract class AbstractReaderTest : AbstractFileTest() {
    @TempDir
    lateinit var dir: Path
    protected val defaultStaleTimeout: Duration = Duration.ofSeconds(1)
    protected lateinit var parser: ContentParser<BufferedReader, RawMessage.Builder>
    protected val onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit = mock { }
    protected val onSourceClosed: (StreamId, Path) -> Unit = mock { }
    protected lateinit var reader: AbstractFileReader<BufferedReader, RawMessage.Builder, MessageID>
    protected lateinit var configuration: CommonFileReaderConfiguration
    protected lateinit var readerState: ReaderState
    private lateinit var directoryChecker: DirectoryChecker

    @BeforeEach
    internal fun setUp() {
        parser = spy(createParser())
        configuration = createConfiguration(defaultStaleTimeout)
        directoryChecker = DirectoryChecker(
            dir,
            createExtractor(),
            {
                it.sortWith(LAST_MODIFICATION_TIME_COMPARATOR
                    .thenComparing { path -> path.nameParts().last().toInt() })
            }
        )

        val movedFileTracker = MovedFileTracker(dir)
        readerState = spy(InMemoryReaderState())
        reader = ProtoDefaultFileReader.Builder(
            configuration,
            directoryChecker,
            parser,
            movedFileTracker,
            readerState = readerState,
            sourceFactory = ::createSource,
            messageIdSupplier = { MessageID.getDefaultInstance() }
        )
            .readFileImmediately()
            .onStreamData(onStreamData)
            .onSourceClosed(onSourceClosed)
            .acceptNewerFiles()
            .setMessageFilters(messageFilters)
            .build()
    }

    protected open fun createParser(): ContentParser<BufferedReader, RawMessage.Builder> = LineParser(lineToBuilder = LineParser.PROTO)

    protected open fun createSource(id: StreamId, path: Path): FileSourceWrapper<BufferedReader> =
        BufferedReaderSourceWrapper(Files.newBufferedReader(path))

    protected open fun createExtractor(): (Path) -> Set<StreamId> = { path ->
        path.nameParts().firstOrNull()?.let { StreamId(it) }?.let {
            setOf(it)
        } ?: emptySet()
    }

    abstract fun createConfiguration(defaultStaleTimeout: Duration): CommonFileReaderConfiguration

    protected open val messageFilters: Collection<ReadMessageFilter<RawMessage.Builder>> = emptyList()

    @AfterEach
    internal fun tearDown() {
        reader.close()
    }
}