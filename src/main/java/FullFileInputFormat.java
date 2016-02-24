import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 *
 * Created by gangadharkadam on 2/23/16.
 */


public class FullFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    //In order to prevent file splitting, override isSplitable() method and return false
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    //Override Create Record Reader to return a custom record reader
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FullFileRecordReader reader = new FullFileRecordReader();
        reader.initialize(split, context);
        return reader;

    }


}
