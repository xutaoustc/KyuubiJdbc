package sparkJdbc;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

public class Main {
    private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";

    // https://kyuubi.apache.org/docs/r1.5.2-incubating/client/hive_jdbc.html
    // https://kyuubi.apache.org/docs/r1.5.2-incubating/deployment/engine_share_level.html
    // https://kyuubi.apache.org/docs/r1.5.2-incubating/deployment/spark/incremental_collection.html
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InterruptedException {

        // hdfs/xxx@yyy /etc/security/keytabs/hdfs.keytab "jdbc:hive2://xxx:2181,xxx1:2181,xxx2:2181,xxx3:2181,xxx4:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi_root/kyuubi?kyuubi.operation.incremental.collect=true" hdfs "select * from test.items" 100
        String principal = args[0]; // kerberos principal
        String keytab = args[1]; // keytab file location
        String kyuubiJdbcUrl = args[2];
        String user = args[3];
        String sql = args[4];
        Integer maxRows = Integer.parseInt(args[5]);

//        Connection conn = DriverManager.getConnection(kyuubiJdbcUrl, user, "");
        Configuration configuration = new Configuration();
        configuration.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

        Class.forName(driverName);
        Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>(){
            public Connection run() throws SQLException {
                return DriverManager.getConnection(kyuubiJdbcUrl, user, "");
            }
        });
        Statement st = conn.createStatement();
        st.setMaxRows(maxRows);
        ResultSet res = st.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        res.close();
        st.close();
        conn.close();
    }

    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
