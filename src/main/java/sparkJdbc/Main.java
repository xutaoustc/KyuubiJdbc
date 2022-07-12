package sparkJdbc;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

public class Main {
    private static String driverName = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InterruptedException {

        String principal = args[0]; // kerberos principal
        String keytab = args[1]; // keytab file location
        String kyuubiJdbcUrl = args[2];
        String user = args[3];
        String sql = args[4];
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
