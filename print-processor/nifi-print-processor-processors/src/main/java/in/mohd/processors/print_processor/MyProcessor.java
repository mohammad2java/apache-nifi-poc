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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@Tags({"example", "file", "directory", "list", "print"})
@CapabilityDescription("Lists the files found in a configured directory, prints every file name to the NiFi log and transfers the "
        + "result to the 'success' relationship so that the next processor in the flow can consume it. Either one FlowFile per file "
        + "is created (the FlowFile content is the file name and its attributes describe the file) or a single FlowFile that contains "
        + "the whole listing, one file name per line.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the listed file (one-FlowFile-per-file mode only)"),
        @WritesAttribute(attribute = "path", description = "The listed file's directory relative to the configured input directory, with a trailing separator"),
        @WritesAttribute(attribute = "absolute.path", description = "The listed file's absolute directory, with a trailing separator"),
        @WritesAttribute(attribute = "file.size", description = "The size of the listed file in bytes"),
        @WritesAttribute(attribute = "file.lastModified", description = "The last modified time of the listed file in ISO-8601 format"),
        @WritesAttribute(attribute = "file.count", description = "The number of files that were listed by this invocation"),
        @WritesAttribute(attribute = "input.directory", description = "The absolute path of the directory that was listed")
})
public class MyProcessor extends AbstractProcessor {

    static final String FILE_COUNT_ATTRIBUTE = "file.count";

    static final String INPUT_DIRECTORY_ATTRIBUTE = "input.directory";

    /**
     * Validates that the configured value points to an existing, readable directory. A value that contains Expression Language is
     * accepted as is, because it can only be resolved when the processor is scheduled.
     */
    public static final Validator DIRECTORY_VALIDATOR = (subject, input, context) -> {
        if (input == null || input.trim().isEmpty()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("A directory must be specified")
                    .valid(false)
                    .build();
        }
        if (input.contains("${")) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }

        final File directory = new File(input.trim());
        if (!directory.exists()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("Directory " + input + " does not exist")
                    .valid(false)
                    .build();
        }
        if (!directory.isDirectory()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation(input + " is not a directory")
                    .valid(false)
                    .build();
        }
        if (!directory.canRead()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("Directory " + input + " is not readable by the NiFi user")
                    .valid(false)
                    .build();
        }

        return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
    };

    public static final AllowableValue OUTPUT_MODE_ONE_PER_FILE = new AllowableValue(
            "one-flowfile-per-file",
            "One FlowFile per File",
            "Create one FlowFile for every listed file. The FlowFile content is the file name and the filename, path, absolute.path, "
                    + "file.size and file.lastModified attributes describe the file.");

    public static final AllowableValue OUTPUT_MODE_SINGLE_LIST = new AllowableValue(
            "single-file-listing",
            "Single FlowFile with File Listing",
            "Create a single FlowFile whose content is the complete list of file names, one file name per line.");

    public static final PropertyDescriptor INPUT_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .displayName("Input Directory")
            .description("The directory whose file names should be listed, for example C:\\data\\inbox")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(DIRECTORY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECURSE_SUBDIRECTORIES = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .displayName("Recurse Subdirectories")
            .description("When true the subdirectories of the input directory are listed as well")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .displayName("File Filter")
            .description("A regular expression that a file name has to match in order to be listed, for example .*\\.csv")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("Output Mode")
            .displayName("Output Mode")
            .description("Determines how the file listing is transferred to the 'success' relationship")
            .required(true)
            .allowableValues(OUTPUT_MODE_ONE_PER_FILE, OUTPUT_MODE_SINGLE_LIST)
            .defaultValue(OUTPUT_MODE_ONE_PER_FILE.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that carry the listed file name(s) to the next processor in the flow")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(INPUT_DIRECTORY, RECURSE_SUBDIRECTORIES, FILE_FILTER, OUTPUT_MODE);

        relationships = Set.of(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Scheduled to list the file names of directory [{}] (recurse subdirectories: {}, file filter: {})",
                context.getProperty(INPUT_DIRECTORY).evaluateAttributeExpressions().getValue(),
                context.getProperty(RECURSE_SUBDIRECTORIES).getValue(),
                context.getProperty(FILE_FILTER).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final File inputDirectory = new File(context.getProperty(INPUT_DIRECTORY).evaluateAttributeExpressions().getValue());
        if (!inputDirectory.isDirectory()) {
            getLogger().error("Directory [{}] does not exist or is not a directory; nothing can be listed",
                    inputDirectory.getAbsolutePath());
            context.yield();
            return;
        }

        final boolean recurse = context.getProperty(RECURSE_SUBDIRECTORIES).asBoolean();
        final Pattern fileFilter = Pattern.compile(context.getProperty(FILE_FILTER).getValue());

        final List<File> files = new ArrayList<>();
        collectFiles(inputDirectory, recurse, fileFilter, files);

        if (files.isEmpty()) {
            getLogger().debug("No file in directory [{}] matches the filter [{}]",
                    inputDirectory.getAbsolutePath(), fileFilter.pattern());
            return;
        }

        getLogger().info("Found {} file(s) in directory [{}]", files.size(), inputDirectory.getAbsolutePath());

        // "print" the name of every listed file
        for (final File file : files) {
            getLogger().info("File name: {}", file.getName());
        }

        if (OUTPUT_MODE_SINGLE_LIST.getValue().equals(context.getProperty(OUTPUT_MODE).getValue())) {
            transferSingleFileListing(session, inputDirectory, files);
        } else {
            transferOneFlowFilePerFile(session, inputDirectory, files);
        }

        session.adjustCounter("Files Listed", files.size(), false);
    }

    /**
     * Creates one FlowFile per listed file, with the file name as content, and transfers every FlowFile to 'success'.
     */
    private void transferOneFlowFilePerFile(final ProcessSession session, final File inputDirectory, final List<File> files) {
        for (final File file : files) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, describeFile(inputDirectory, file, files.size()));

            final byte[] content = file.getName().getBytes(StandardCharsets.UTF_8);
            flowFile = session.write(flowFile, outputStream -> outputStream.write(content));

            session.getProvenanceReporter().create(flowFile, "Listed file " + file.getAbsolutePath());
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    /**
     * Creates a single FlowFile that contains the whole listing, one file name per line, and transfers it to 'success'.
     */
    private void transferSingleFileListing(final ProcessSession session, final File inputDirectory, final List<File> files) {
        final StringBuilder listing = new StringBuilder();
        for (final File file : files) {
            listing.append(file.getName()).append('\n');
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FILE_COUNT_ATTRIBUTE, String.valueOf(files.size()));
        attributes.put(INPUT_DIRECTORY_ATTRIBUTE, inputDirectory.getAbsolutePath());

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);

        final byte[] content = listing.toString().getBytes(StandardCharsets.UTF_8);
        flowFile = session.write(flowFile, outputStream -> outputStream.write(content));

        session.getProvenanceReporter().create(flowFile,
                "Listed " + files.size() + " file(s) from " + inputDirectory.getAbsolutePath());
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static Map<String, String> describeFile(final File inputDirectory, final File file, final int fileCount) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), file.getName());
        attributes.put(CoreAttributes.PATH.key(), toRelativePath(inputDirectory, file));
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), toAbsolutePath(file));
        attributes.put("file.size", String.valueOf(file.length()));
        attributes.put("file.lastModified", Instant.ofEpochMilli(file.lastModified()).toString());
        attributes.put(FILE_COUNT_ATTRIBUTE, String.valueOf(fileCount));
        attributes.put(INPUT_DIRECTORY_ATTRIBUTE, inputDirectory.getAbsolutePath());
        return attributes;
    }

    /**
     * Adds the files of the given directory to {@code collected}; when {@code recurse} is true subdirectories are visited as well.
     */
    private void collectFiles(final File directory, final boolean recurse, final Pattern fileFilter, final List<File> collected) {
        final File[] children = directory.listFiles();
        if (children == null) {
            getLogger().error("Unable to read the contents of directory [{}]; check that the NiFi user has read permission",
                    directory.getAbsolutePath());
            return;
        }

        // sorted by name so that the listing, and therefore the result of a flow run, is deterministic
        Arrays.sort(children, Comparator.comparing(File::getName));

        for (final File child : children) {
            if (child.isDirectory()) {
                if (recurse) {
                    collectFiles(child, recurse, fileFilter, collected);
                }
            } else if (fileFilter.matcher(child.getName()).matches()) {
                collected.add(child);
            }
        }
    }

    /**
     * Returns the directory of the given file relative to the input directory, with a trailing separator. An empty string is
     * returned for files that live directly inside the input directory.
     */
    private static String toRelativePath(final File inputDirectory, final File file) {
        final File parent = file.getParentFile();
        if (parent == null) {
            return "";
        }

        final String absoluteParent = parent.getAbsolutePath();
        final String absoluteInputDirectory = inputDirectory.getAbsolutePath();
        if (absoluteParent.equals(absoluteInputDirectory)) {
            return "";
        }

        String relative = absoluteParent.startsWith(absoluteInputDirectory)
                ? absoluteParent.substring(absoluteInputDirectory.length())
                : absoluteParent;
        while (relative.startsWith(File.separator)) {
            relative = relative.substring(File.separator.length());
        }
        return relative.isEmpty() ? "" : relative + File.separator;
    }

    private static String toAbsolutePath(final File file) {
        final File parent = file.getParentFile();
        return parent == null ? "" : parent.getAbsolutePath() + File.separator;
    }
}
