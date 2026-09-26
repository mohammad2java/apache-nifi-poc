/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.mohd.processors.print_processor;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MyProcessorTest {

    private TestRunner testRunner;

    @TempDir
    private Path inputDirectory;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
        testRunner.setProperty(MyProcessor.INPUT_DIRECTORY, inputDirectory.toAbsolutePath().toString());
    }

    @Test
    public void testOneFlowFilePerFileIsTransferredWithTheFileNameAsContent() throws IOException {
        createFile("a.txt", "aaa");
        createFile("b.txt", "bbbb");

        testRunner.run(1, false);

        testRunner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS);
        assertEquals(List.of("a.txt", "b.txt"), fileNames(flowFiles));

        final Map<String, String> expectedSizes = Map.of("a.txt", "3", "b.txt", "4");
        for (final MockFlowFile flowFile : flowFiles) {
            final String fileName = flowFile.getAttribute("filename");
            flowFile.assertContentEquals(fileName);
            flowFile.assertAttributeEquals("path", "");
            flowFile.assertAttributeEquals("absolute.path", inputDirectory.toAbsolutePath() + File.separator);
            flowFile.assertAttributeEquals("file.size", expectedSizes.get(fileName));
            flowFile.assertAttributeEquals("file.count", "2");
            flowFile.assertAttributeEquals("input.directory", inputDirectory.toAbsolutePath().toString());
            flowFile.assertAttributeExists("file.lastModified");
        }
    }

    @Test
    public void testFileFilterIsAppliedToFileNames() throws IOException {
        createFile("report.csv", "");
        createFile("debug.log", "");
        testRunner.setProperty(MyProcessor.FILE_FILTER, ".*\\.csv");

        testRunner.run(1, false);

        testRunner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 1);
        assertEquals(List.of("report.csv"), fileNames(testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS)));
    }

    @Test
    public void testSingleFlowFileContainsTheWholeFileListing() throws IOException {
        createFile("b.txt", "");
        createFile("a.txt", "");
        testRunner.setProperty(MyProcessor.OUTPUT_MODE, MyProcessor.OUTPUT_MODE_SINGLE_LIST);

        testRunner.run(1, false);

        testRunner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS).get(0);
        assertEquals("a.txt\nb.txt\n", new String(testRunner.getContentAsByteArray(flowFile), StandardCharsets.UTF_8));
        flowFile.assertAttributeEquals("file.count", "2");
        flowFile.assertAttributeEquals("input.directory", inputDirectory.toAbsolutePath().toString());
        // note: MockFlowFile always carries a default 'filename' attribute, so it cannot be asserted as absent here
    }

    @Test
    public void testSubdirectoriesAreIgnoredByDefault() throws IOException {
        createFile("root.txt", "");
        createFile("sub/nested.txt", "");

        testRunner.run(1, false);

        testRunner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 1);
        assertEquals(List.of("root.txt"), fileNames(testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS)));
    }

    @Test
    public void testSubdirectoriesAreListedWhenRecursionIsEnabled() throws IOException {
        createFile("root.txt", "");
        createFile("sub/nested.txt", "nested");
        testRunner.setProperty(MyProcessor.RECURSE_SUBDIRECTORIES, "true");

        testRunner.run(1, false);

        testRunner.assertAllFlowFilesTransferred(MyProcessor.REL_SUCCESS, 2);
        final Map<String, MockFlowFile> flowFilesByFileName = testRunner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS)
                .stream()
                .collect(Collectors.toMap(flowFile -> flowFile.getAttribute("filename"), flowFile -> flowFile));
        assertEquals(Set.of("root.txt", "nested.txt"), flowFilesByFileName.keySet());

        final MockFlowFile nested = flowFilesByFileName.get("nested.txt");
        nested.assertContentEquals("nested.txt");
        nested.assertAttributeEquals("path", "sub" + File.separator);
        nested.assertAttributeEquals("absolute.path", inputDirectory.resolve("sub").toAbsolutePath() + File.separator);
        nested.assertAttributeEquals("file.size", "6");

        flowFilesByFileName.get("root.txt").assertAttributeEquals("path", "");
    }

    @Test
    public void testNoFlowFileIsTransferredForAnEmptyDirectory() {
        testRunner.run(1, false);

        testRunner.assertTransferCount(MyProcessor.REL_SUCCESS, 0);
    }

    @Test
    public void testMissingDirectoryIsNotValid() {
        final String missingDirectory = inputDirectory.resolve("does-not-exist").toAbsolutePath().toString();

        final ValidationResult result = testRunner.setProperty(MyProcessor.INPUT_DIRECTORY, missingDirectory);

        assertFalse(result.isValid(), "A directory that does not exist must not be valid");
        testRunner.assertNotValid();
    }

    private void createFile(final String relativePath, final String content) throws IOException {
        final Path file = inputDirectory.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private static List<String> fileNames(final List<MockFlowFile> flowFiles) {
        return flowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .sorted()
                .collect(Collectors.toList());
    }
}
