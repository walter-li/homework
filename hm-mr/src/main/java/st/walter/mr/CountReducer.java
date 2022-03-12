package st.walter.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean phone = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (FlowBean value : values) {
            up = up + value.getUp();
            down = down + value.getDown();
        }
        phone.setUp(up);
        phone.setDown(down);
        phone.setTotalSize(up+down);
        context.write(key, phone);

    }
}
