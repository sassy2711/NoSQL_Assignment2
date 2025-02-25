// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.Mapper;
// import java.io.IOException;
// import org.apache.commons.text.StringEscapeUtils; // Apache Commons for HTML entity decoding

// public class WikiMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
//     private IntWritable indexKey = new IntWritable();

//     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//         String line = StringEscapeUtils.unescapeHtml4(value.toString().trim()); // Decode HTML entities
//         String[] words = line.split("\\s+");

//         if (words.length < 2) return; // Ignore empty or malformed lines

//         // Extract and clean document ID
//         String docID = words[0].replaceAll("[^a-zA-Z0-9]", ""); 
//         if (docID.isEmpty()) return; // Skip if docID is empty

//         // Emit (index, "docID:word")
//         for (int i = 1; i < words.length; i++) {
//             String word = words[i];
//             word = StringEscapeUtils.unescapeHtml4(word); // Decode any remaining HTML entities
//             word = word.replaceAll("[^a-zA-Z0-9]", ""); // Clean non-alphanumeric characters

//             if (!word.isEmpty()) { // Avoid empty words
//                 indexKey.set(i);
//                 context.write(indexKey, new Text(docID + ":" + word));
//             }
//         }
//     }
// }

// WikiMapper.java
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.text.StringEscapeUtils;
import java.io.IOException;

public class WikiMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable indexKey = new IntWritable();
    private String documentID; // Stores the filename-based document ID

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Extract filename from FileSplit (keeps numbers and ".txt")
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        documentID = fileSplit.getPath().getName(); 

        // Debugging output
        System.out.println("Processing file: " + documentID);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = StringEscapeUtils.unescapeHtml4(value.toString().trim());

        if (line.isEmpty()) return; // Skip empty lines

        String[] words = line.split("\\s+");

        // Emit (index, "documentID:word"), skipping invalid words
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            word = StringEscapeUtils.unescapeHtml4(word); 
            word = word.replaceAll("[^a-zA-Z]", ""); 

            if (!word.isEmpty()) { 
                indexKey.set(i + 1); // Start index from 1
                context.write(indexKey, new Text(documentID + ":" + word));
            }
        }
    }
}
