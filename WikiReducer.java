// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.Reducer;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import org.apache.commons.text.StringEscapeUtils; // Apache Commons for HTML entity decoding

// public class WikiReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//     public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//         List<String> wordList = new ArrayList<>();

//         for (Text val : values) {
//             String entry = StringEscapeUtils.unescapeHtml4(val.toString()); // Decode HTML entities
//             if (entry.contains(":")) { // Ensure proper format
//                 wordList.add(entry);
//             }
//         }

//         // Sort based on document ID to maintain order
//         Collections.sort(wordList);

//         // Emit (index, (doc-ID, word))
//         for (String entry : wordList) {
//             String[] parts = entry.split(":", 2); // Split only at the first ':'
//             if (parts.length < 2) continue; // Ignore malformed inputs

//             String docID = StringEscapeUtils.unescapeHtml4(parts[0].replaceAll("[^a-zA-Z0-9]", "")); // Clean and decode docID
//             String word = StringEscapeUtils.unescapeHtml4(parts[1].replaceAll("[^a-zA-Z0-9]", "")); // Clean and decode word

//             if (!docID.isEmpty() && !word.isEmpty()) {
//                 context.write(key, new Text("(" + docID + ", " + word + ")"));
//             }
//         }
//     }
// }

// WikiReducer.java
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.text.StringEscapeUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WikiReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> wordList = new ArrayList<>();

        for (Text val : values) {
            String entry = StringEscapeUtils.unescapeHtml4(val.toString()); 
            if (entry.contains(":")) { 
                wordList.add(entry);
            }
        }

        // Sort based on document ID for consistency
        Collections.sort(wordList);

        // Emit (index, (documentID, word)) without altering documentID
        for (String entry : wordList) {
            String[] parts = entry.split(":", 2); 
            if (parts.length < 2) continue; 

            String docID = parts[0]; // Keep documentID exactly as it is (e.g., "123.txt")
            String word = parts[1].replaceAll("[^a-zA-Z]", ""); 

            if (!docID.isEmpty() && !word.isEmpty()) {
                context.write(key, new Text("(" + docID + ", " + word + ")"));
            }
        }
    }
}
