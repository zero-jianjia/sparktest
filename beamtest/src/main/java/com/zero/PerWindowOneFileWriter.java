package com.zero;

import com.google.common.base.Verify;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

public class PerWindowOneFileWriter extends PTransform<PCollection<String>, PDone> {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
    private String filenamePrefix;
    private Integer numShards;

    public PerWindowOneFileWriter(String filenamePrefix, Integer numShards) {
        this.filenamePrefix = filenamePrefix;
        this.numShards = numShards;
    }

    @Override
    public PDone expand(PCollection<String> input) {
        String prefix = "";
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
        if (!resource.isDirectory()) {
            prefix = Verify.verifyNotNull(resource.getFilename(),
                    "A non-directory resource should have a non-null filename: %s",
                    resource);
        }

        TextIO.Write write = TextIO.write()
                .to(resource.getCurrentDirectory())
                .withFilenamePolicy(new PerWindowFiles(prefix))
                .withWindowedWrites();
        write = write.withNumShards(numShards == null ? 1 : numShards);
        return input.apply(write);
    }

    public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

        private final String prefix;

        public PerWindowFiles(String prefix) {
            this.prefix = prefix;
        }

        private String generateFilenamePrefix(IntervalWindow window) {
            return String.format("%s_%s-%s", prefix,
                    FORMATTER.print(window.start()),
                    FORMATTER.print(window.end()));
        }

        @Override
        public ResourceId windowedFilename(
                ResourceId outputDirectory, WindowedContext context, String extension) {
            IntervalWindow window = (IntervalWindow) context.getWindow();
            int numShards = context.getNumShards();
            String filename;
            String prefix = generateFilenamePrefix(window);
            if(numShards == 1) {
                filename = String.format("%s", prefix);
            } else {
                filename = String.format("%s_%s_%s%s",
                        prefix, context.getShardNumber(), context.getNumShards(), extension);
            }
            return outputDirectory.resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(
                ResourceId outputDirectory, Context context, String extension) {
            throw new UnsupportedOperationException("Unsupported.");
        }

    }
}