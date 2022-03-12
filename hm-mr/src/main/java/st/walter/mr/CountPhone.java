package st.walter.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountPhone {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setMapperClass(CountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Phone.class);

            job.setReducerClass(CountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Phone.class);


            job.setJarByClass(CountPhone.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            Boolean flag = job.waitForCompletion(true);
            System.exit(flag ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
