// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// public class WikiDriver {
//     public static void main(String[] args) throws Exception {
//         if (args.length != 2) {
//             System.err.println("Usage: WikiDriver <input path> <output path>");
//             System.exit(-1);
//         }

//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "Wikipedia Word Indexing");

//         job.setJarByClass(WikiDriver.class);
//         job.setMapperClass(WikiMapper.class);
//         job.setReducerClass(WikiReducer.class);

//         job.setOutputKeyClass(IntWritable.class);
//         job.setOutputValueClass(Text.class);

//         FileInputFormat.addInputPath(job, new Path(args[0]));
//         FileOutputFormat.setOutputPath(job, new Path(args[1]));

//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) { // Now allows 2 or 3 args
            System.err.println("Usage: WikiDriver <input path> <output path> [commons-text-jar]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // If a third argument (JAR path) is provided, add it to classpath
        if (args.length == 3) {
            conf.set("mapreduce.job.classpath.files", args[2]); // Ensure JAR is available in Hadoop runtime
        }

        Job job = Job.getInstance(conf, "Wikipedia Word Indexing");

        job.setJarByClass(WikiDriver.class);
        job.setMapperClass(WikiMapper.class);
        job.setReducerClass(WikiReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
