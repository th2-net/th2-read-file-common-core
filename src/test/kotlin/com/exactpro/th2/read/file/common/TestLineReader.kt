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

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration
import com.exactpro.th2.read.file.common.impl.BufferedReaderSourceWrapper
import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

class TestLineReader(
    configuration: CommonFileReaderConfiguration,
    directoryChecker: DirectoryChecker,
    contentParser: ContentParser<BufferedReader>,
    onStreamData: (StreamId, List<RawMessage.Builder>) -> Unit
) : AbstractFileReader<BufferedReader>(
    configuration,
    directoryChecker,
    contentParser,
    onStreamData,
) {
    override fun canReadRightNow(holder: FileHolder<BufferedReader>, staleTimeout: Duration): Boolean = true

    override fun acceptFile(streamId: StreamId, currentFile: Path?, newFile: Path): Boolean =
        currentFile == null
            || currentFile.attributes.creationTime() <= newFile.attributes.creationTime()

    override fun createSource(streamId: StreamId, path: Path): FileSourceWrapper<BufferedReader> =
        BufferedReaderSourceWrapper(Files.newBufferedReader(path))
}