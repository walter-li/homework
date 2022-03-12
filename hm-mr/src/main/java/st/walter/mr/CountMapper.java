package st.walter.mr;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] params = val.split("\t");
        String phoneNum = params[1];
        String up = params[8];
        String down = params[9];
        FlowBean ph = new FlowBean();
        ph.setUp(Long.parseLong(up));
        ph.setDown(Long.parseLong(down));
        ph.setTotalSize(ph.getUp()+ph.getDown());
        context.write(new Text(phoneNum), ph);
    }
}
