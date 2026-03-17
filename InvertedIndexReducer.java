import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashSet<String> docSet = new HashSet<>();

        for (Text val : values) {
            docSet.add(val.toString());
        }

        String result = String.join(", ", docSet);
        context.write(key, new Text(result));
    }
}
