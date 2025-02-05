import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class TaskB {
    public static class MostAccessedMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String pid = fields[2].trim();
                context.write(new Text(pid), new Text("a;"));
            }
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String perId = fields[0].trim();
                String name = fields[1].trim();
                String nat = fields[3].trim();
                context.write(new Text(perId), new Text("d;" + name + "," + nat));
            }
        }
    }

    public static class MostAccessedReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Integer, String> pages = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {
            int count = 0;
            String details = "";
            for(Text val : values){
                String[] parts = val.toString().split(";");
                if(parts[0].equals("a")){
                    count++;
                } else if(parts[0].equals("d")){
                    details = parts[1];
                }
            }

            pages.put(count, details);

            if(pages.size() > 10){
                pages.remove(pages.lastKey());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<Integer, String> entry : pages.entrySet()){
                context.write(new Text(entry.getKey().toString()), new Text(entry.getValue()));
            }
        }
    }
}
