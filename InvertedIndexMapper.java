import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text docID = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit)
                context.getInputSplit()).getPath().getName();

        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", "");

        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            docID.set(fileName);
            context.write(word, docID);
        }
    }
}
