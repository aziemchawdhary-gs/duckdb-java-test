package org.finos.legend;

import java.sql.*;
import org.duckdb.DuckDBConnection;

import static java.lang.Thread.sleep;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException {

        String  connectURL = "jdbc:duckdb:";
        int  MAX_THREADS = 5;

        // Create 100 threads
        Thread[] threads = new Thread[MAX_THREADS];

        Class.forName("org.duckdb.DuckDBDriver");

        Connection conn = DriverManager.getConnection(connectURL, "", "");
        conn.setAutoCommit(false);
        conn.createStatement().execute("Create TABLE lotsOfConnection (id int)");
        conn.commit();

        sleep(10);
        for (int i = 0; i < threads.length; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    Connection conn2 = ((DuckDBConnection) conn).duplicate();
//                    Connection conn2 = DriverManager.getConnection(connectURL, "", "");
                    conn2.setAutoCommit(false);
                    Statement stmt = conn2.createStatement();
                    stmt.execute("insert into lotsOfConnection values(" + finalI + ")");
                    conn2.commit();
                    conn2.close();
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        // Waiting for all threads completed
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int expectedSumValue = 0;
        for (int i = 0; i < threads.length; i++) {
            expectedSumValue = expectedSumValue + i;
        }
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*),SUM(id) from lotsOfConnection");
        while (rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getInt(2));
        }
        rs.close();
        System.out.println( "Hello World!" );
    }
}
