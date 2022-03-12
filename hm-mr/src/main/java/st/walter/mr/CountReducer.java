package st.walter.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, Phone, Text, Phone> {
    private Phone phone = new Phone();

    @Override
    protected void reduce(Text key, Iterable<Phone> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (Phone value : values) {
            up = up + value.getUp();
            down = down + value.getDown();
        }
        phone.setUp(up);
        phone.setDown(down);
        phone.setTotalSize(up+down);
        context.write(key, phone);

    }
}
