package SameNationality;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SameNationalityMapper extends Mapper<Object, Text, Text, Text> {
    private static final String NATIONALITY = "Grenada";

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] attributes = line.split(",");

        if (attributes.length >= 5 && NATIONALITY.equals(attributes[2].trim())) {
            context.write(new Text(attributes[1]), new Text(attributes[4]));
        }
    }
}

