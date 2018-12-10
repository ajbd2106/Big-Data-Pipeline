package mjaksic.from_kafka_to_hive;

import java.io.Closeable;
import java.sql.*;

/**
 * Connects to the Hive database using JDBC. An experimental class.
 */
public class HiveJDBC implements Closeable {
    private static final int exception_exit_code = -4;

    private Connection hive_connection = null;
    private Statement statement = null;

    public HiveJDBC() {
        CheckDriver();
        SetHiveConnection();
        SetStatement();
    }

    /**
     * Attempts to check if the JDBC Hive Driver exists.
     */
    private void CheckDriver() {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            System.out.println("Driver found");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            ShutDownAfterException();
        }
    }

    /**
     * Attempts to establish a connection with Hive.
     */
    private void SetHiveConnection() {
        try {
            Connection hive_connection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "queries_user_name", "");

            this.hive_connection = hive_connection;
            System.out.println("Connection created");

        } catch (SQLException e) {
            e.printStackTrace();
            ShutDownAfterException();
        }
    }

    /**
     * Attempts to create a statement.
     */
    private void SetStatement() {
        try {
            Statement statement = this.hive_connection.createStatement();

            this.statement = statement;
            System.out.println("Statement created");

        } catch (SQLException e) {
            e.printStackTrace();
            CloseConnection();
            ShutDownAfterException();
        }
    }



    public void Run() {
        String select_columns = "ip";
        String select_table = "unpartitioned_alarms_raw";
        String sql_select = "SELECT " + select_columns + " FROM " + select_table + " GROUP BY " + select_columns;

        ResultSet select_results = ExecuteSQL(sql_select);
    }

    /**
     * Attempts to execute a SQL statement.
     * @param select_string Static SQL string.
     * @return Query results.
     */
    public ResultSet ExecuteSQL(String select_string) {
        try {
            return this.statement.executeQuery(select_string);
        } catch (SQLException e) {
            e.printStackTrace();
            CloseConnection();
            ShutDownAfterException();
        }
        return null;
    }



    private void ShutDownAfterException() {
        System.out.println("Shutting down after an exception.");
        System.exit(HiveJDBC.exception_exit_code);
    }

    private void CloseConnection() {
        try {
            hive_connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * You may want to use try-with-resources.
     */
    @Override
    public void close() {
        CloseConnection();
    }
}
