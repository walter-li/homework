package st.walter.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTableOpertation {

    public static void main(String[] args) throws IOException {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, args[0]);
        configuration.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000");
        configuration.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
        HBaseClient client = new HBaseClient(configuration);
        //创建namespace
        try {
            client.createNameSpace("lichengjun");
        } catch (Exception e) {

            System.out.println(e);
        }
        //创建表
        try {
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
            ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.of("info");
            ColumnFamilyDescriptor score = ColumnFamilyDescriptorBuilder.of("score");
            columnFamilyDescriptorList.add(info);
            columnFamilyDescriptorList.add(score);
            client.createTable("lichengjun", "student", columnFamilyDescriptorList);
        } catch (Exception e) {
            System.out.println(e);
        }

        //插入数据
        List<Put> puts = new ArrayList<>();
        Put put1 = new Put("lichengjun".getBytes());
        put1.addColumn("info".getBytes(), "student_id".getBytes(), "G20220735020181".getBytes());
        put1.addColumn("info".getBytes(), "class".getBytes(), "1".getBytes());
        put1.addColumn("score".getBytes(), "understanding".getBytes(), "100".getBytes());
        put1.addColumn("score".getBytes(), "programming".getBytes(), "100".getBytes());
        puts.add(put1);

        Put put2 = new Put("lichengjun2".getBytes());
        put2.addColumn("info".getBytes(), "student_id".getBytes(), "G20220735020181".getBytes());
        put2.addColumn("info".getBytes(), "class".getBytes(), "1".getBytes());
        put2.addColumn("score".getBytes(), "understanding".getBytes(), "100".getBytes());
        put2.addColumn("score".getBytes(), "programming".getBytes(), "100".getBytes());
        puts.add(put2);

        Put put3 = new Put("Tom".getBytes());
        put3.addColumn("info".getBytes(), "student_id".getBytes(), "20210000000001".getBytes());
        put3.addColumn("info".getBytes(), "class".getBytes(), "1".getBytes());
        put3.addColumn("score".getBytes(), "understanding".getBytes(), "75".getBytes());
        put3.addColumn("score".getBytes(), "programming".getBytes(), "82".getBytes());
        puts.add(put3);

        Put put4 = new Put("Jerry".getBytes());
        put4.addColumn("info".getBytes(), "student_id".getBytes(), "20210000000002".getBytes());
        put4.addColumn("info".getBytes(), "class".getBytes(), "1".getBytes());
        put4.addColumn("score".getBytes(), "understanding".getBytes(), "85".getBytes());
        put4.addColumn("score".getBytes(), "programming".getBytes(), "67".getBytes());
        puts.add(put4);


        Put put5 = new Put("Jack".getBytes());
        put5.addColumn("info".getBytes(), "student_id".getBytes(), "20210000000003".getBytes());
        put5.addColumn("info".getBytes(), "class".getBytes(), "2".getBytes());
        put5.addColumn("score".getBytes(), "understanding".getBytes(), "80".getBytes());
        put5.addColumn("score".getBytes(), "programming".getBytes(), "80".getBytes());
        puts.add(put5);


        Put put6 = new Put("Rose".getBytes());
        put6.addColumn("info".getBytes(), "student_id".getBytes(), "20210000000004".getBytes());
        put6.addColumn("info".getBytes(), "class".getBytes(), "2".getBytes());
        put6.addColumn("score".getBytes(), "understanding".getBytes(), "60".getBytes());
        put6.addColumn("score".getBytes(), "programming".getBytes(), "61".getBytes());
        puts.add(put6);

        client.insert("lichengjun", "student", puts);
        //查询数据
        Result result = client.getByKey("lichengjun", "student", "lichengjun");
        System.out.println("查询lichengjun的row=" + String.valueOf(result.getRow()));
        //删除数据
        Delete delete = new Delete("lichengjun2".getBytes());

        client.deleteByKey("lichengjun", "student", delete);
        Result result2 = client.getByKey("lichengjun", "student", "lichengjun2");
        System.out.println("查询lichengjun2是否存在" + result2.getExists());
        client.closeConnection();
    }
}
