package st.walter.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HBaseClient {
    private Connection connection;

    public HBaseClient(Configuration configuration) throws IOException {
        init(configuration);
    }

    private void init(Configuration configuration) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        this.connection = connection;
    }

    /**
     * 创建namespace
     *
     * @param nameSpace
     */
    public void createNameSpace(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);
    }

    /**
     * 创建table
     *
     * @param nameSpace
     * @param tableName
     * @throws IOException
     */
    public void createTable(String nameSpace, String tableName, Collection<ColumnFamilyDescriptor> columnFamilyDescriptors) throws IOException {
        Admin admin = connection.getAdmin();
        final TableName table = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).setColumnFamilies(columnFamilyDescriptors).build();
        admin.createTable(tableDescriptor);
    }


    public void deleteTable(String nameSpace, String tableName) throws Exception {
        Admin admin = connection.getAdmin();
        if (exist(nameSpace, tableName)) {
            TableName table = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
            admin.deleteTable(table);
        } else {
            throw new Exception("表不存在");
        }
    }

    private boolean exist(String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
        return admin.tableExists(table);
    }


    public Result getByKey(String nameSpace,String tableName, String key) throws IOException {
        TableName tablename = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
        Table table = connection.getTable(tablename);
        Get get = new Get(key.getBytes());
        return table.get(get);
    }

    public void deleteByKey(String nameSpace, String tableName, Delete delete) throws IOException {
        TableName tablename = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
        Table table = connection.getTable(tablename);
        table.delete(delete);
    }

    public void insert(String nameSpace,String tableName,List<Put> puts) throws IOException {
        TableName tablename = TableName.valueOf(nameSpace + TableName.NAMESPACE_DELIM + tableName);
        Table table = connection.getTable(tablename);
        table.put(puts);
    }

    public void closeConnection() throws IOException {
        Admin admin = connection.getAdmin();
        admin.close();
        this.connection.close();
    }


}
