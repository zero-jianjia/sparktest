//package com.zero;
//
//import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.*;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.PCollectionView;
//import org.apache.beam.sdk.values.PDone;
//
//import java.io.File;
//import java.util.Map;
//
///**
// * @author yinchen
// */
//public class Main {
//    public static void main(String[] args) {
//        args = new String[]{
//                "—-ipFile=/Users/yanjun/Data/beam/events/ips.txt",
//                "—-eventFile=/Users/yanjun/Data/beam/events/apache_event_20171210.log",
//                "—-outputDir=/Users/yanjun/Data/beam/events/output/",
//                "—-outputFilePrefix=result",
//                "—-windowSizeSecs=300",
//                "—-numShards=1"
//        };
//
//
//        WindowingOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
//                .as(WindowingOptions.class);
//
//        String ipFile = options.getIpFile();
//        String eventFile = options.getEventFile();
//        String output = new File(options.getOutputDir(),
//                options.getOutputFilePrefix()).getAbsolutePath();
//
//
//        final PCollectionView<Map<String, String>> ipToAreaMapView =
//                pipeline.apply(TextIO.read().from(ipFile))
//                        .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
//                            @ProcessElement
//                            public void processElement(ProcessContext c) {
//                                String[] ipAreaPair = c.element().split("\t");
//                                if (ipAreaPair.length == 2) {
//                                    c.output(KV.of(ipAreaPair[0], ipAreaPair[1]));
//                                }
//                            }
//                        })).apply(View.<String, String>asMap());
//
//
//        // control to output final result
//        final PTransform<PCollection<String>, PDone> writer =
//                new PerWindowOneFileWriter(output, options.getNumShards());
//
//// format & output windowed result
//        areaCounts.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//            @Override
//            public String apply(KV<String, Long> input) {
//                return input.getKey() + "\t" + input.getValue();
//            }
//        })).apply(writer);
//    }
//}
