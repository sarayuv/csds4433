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
                String pageId = fields[2].trim();
                context.write(new Text(pageId), new Text("a;"));
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
                String pageId   = fields[0].trim();
                String name     = fields[1].trim();
                String nat      = fields[3].trim();

                context.write(new Text(pageId), new Text("d;" + name + "," + nat));
            }
        }
    }

    public static class PageCount {
        int count;
        String details;

        public PageCount(int count, String details) {
            this.count = count;
            this.details = details;
        }
    }

    public static class MostAccessedReducer extends Reducer<Text, Text, Text, Text> {
        private PriorityQueue<PageCount> topPages;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            topPages = new PriorityQueue<>(10, Comparator.comparingInt(pc -> pc.count));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String details = "";

            for (Text val : values) {
                String[] parts = val.toString().split(";");

                if (parts.length > 0) {
                    if (parts[0].equals("a")) {
                        count++;
                    } else if (parts[0].equals("d")) {
                        details = parts[1];
                    }
                }
            }

            if (topPages.size() < 10) {

                topPages.add(new PageCount(count, details));
            } else {

                if (!topPages.isEmpty() && topPages.peek().count < count) {
                    topPages.poll();
                    topPages.add(new PageCount(count, details));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<PageCount> resultList = new ArrayList<>();
            while (!topPages.isEmpty()) {
                resultList.add(topPages.poll());
            }

            Collections.reverse(resultList);

            for (PageCount pc : resultList) {
                context.write(new Text(String.valueOf(pc.count)), new Text(pc.details));
            }
        }
    }
}
