package com.zero;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

interface WindowingOptions extends PipelineOptions {

    @Description("Path of the IP library file to read from.")
    String getIpFile();
    void setIpFile(String ipFile);

    @Description("Path of the event log file to read from.")
    String getEventFile();
    void setEventFile(String eventFile);

    @Description("Fixed window duration, in seconds.")
    @Default.Integer(5)
    Integer getWindowSizeSecs();
    void setWindowSizeSecs(Integer value);

    @Description("Fixed number of shards to produce per window.")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer numShards);

    @Description("Directory of the output to write to.")
    String getOutputDir();
    void setOutputDir(String outputDir);

    @Description("Prefix of the output file prefix.")
    @Default.String("result")
    String getOutputFilePrefix();
    void setOutputFilePrefix(String outputFilePrefix);
}
