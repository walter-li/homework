package st.walter.mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private Long up;
    private Long down;
    private Long totalSize;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(totalSize);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        up = dataInput.readLong();
        down = dataInput.readLong();
        totalSize = dataInput.readLong();
    }


    public Long getUp() {
        return up;
    }

    public FlowBean setUp(Long up) {
        this.up = up;
        return this;
    }

    public Long getDown() {
        return down;
    }

    public FlowBean setDown(Long down) {
        this.down = down;
        return this;
    }

    public Long getTotalSize() {
        return totalSize;
    }

    public FlowBean setTotalSize(Long totalSize) {
        this.totalSize = totalSize;
        return this;
    }

    @Override
    public String toString() {
        return up+"\t"+down+"\t"+totalSize;
    }

    @Override
    public int compareTo(FlowBean o) {
        return (int) (o.totalSize-this.totalSize);
    }
}
