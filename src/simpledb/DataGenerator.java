package simpledb;import java.sql.Connection;import java.sql.Driver;import java.sql.SQLException;import java.sql.Statement;import java.util.Random;import simpledb.remote.SimpleDriver;/** * CS 4432 Project 2 * * We added this class from the given code for data generation for the index * information and extensible hash index testing. * * @author Aditya Nivarthi */public class DataGenerator {	final static int maxSize = 30000; // We used a smaller value than 100,000	/**	 * Main data generation method for creating tables and adding records.	 *	 * @param args	 */	public static void main(String[] args) {		Connection conn = null;		Driver d = new SimpleDriver();		// you may change it if your SimpleDB server is running on a different		// machine		String host = "localhost";		String url = "jdbc:simpledb://" + host;		Random rand = null;		Statement s = null;		try {			conn = d.connect(url, null);			s = conn.createStatement();			/**			 * CS 4432 Project 2			 *			 * These commands are for creating the tables			 *			 * Table test1 has a hash index			 *			 * Table test2 has an extensible hash index			 *			 * Table test3 has a B tree index			 *			 * Table test4 has no index			 *			 * Table test5 has fewer records and no index			 */			s.executeUpdate("Create table test1" + "( a1 int," + " a2 int" + ")");			s.executeUpdate("Create table test2" + "( a1 int," + " a2 int" + ")");			s.executeUpdate("Create table test3" + "( a1 int," + " a2 int" + ")");			s.executeUpdate("Create table test4" + "( a1 int," + " a2 int" + ")");			s.executeUpdate("Create table test5" + "( a1 int," + " a2 int" + ")");			/**			 * CS 4432 Project 2 These commands are for creating the indices on			 * the test tables.			 */			s.executeUpdate("create sh index idx1 on test1 (a1)");			s.executeUpdate("create eh index idx2 on test2 (a1)");			s.executeUpdate("create bt index idx3 on test3 (a1)");			/**			 * CS 4432 Project 2			 *			 * This was used for basic testing of the extensible hash index			 */			// rand = new Random(1);			// for (int i = 0; i < 5; i++) {			// s.executeUpdate(			// "insert into test2 (a1,a2) values(" + rand.nextInt(1000) + "," +			// rand.nextInt(1000) + ")");			// }			// int i = 1;			for (int i = 1; i < 6; i++) {				if (i != 5) {					// ensure every table gets the same data					rand = new Random(1);					for (int j = 0; j < maxSize; j++) {						s.executeUpdate("insert into test" + i + " (a1,a2) values(" + rand.nextInt(1000) + ","								+ rand.nextInt(1000) + ")");					}				} else {					// case where i=5					// insert 10000 records into test5					for (int j = 0; j < maxSize / 2; j++) {						s.executeUpdate("insert into test" + i + " (a1,a2) values(" + j + "," + j + ")");					}				}				System.out.println("Table " + i + " complete.");			}			/**			 * These queries are for doing 1 predicate selections from the			 * tables			 */			// s.executeQuery("select a1, a2 from test1 where a1 = 978");			// s.executeQuery("select a1, a2 from test2 where a1 = 978");			// s.executeQuery("select a1, a2 from test4 where a1 = 978");			// s.executeQuery("select a1, a2 from test5 where a1 = 978");			/**			 * These queries are for doing 2 predicate selections from the			 * tables			 */			// s.executeQuery("select a1, a2 from test1 where a1 = 978 and a2 =			// 588");			// s.executeQuery("select a1, a2 from test2 where a1 = 978 and a2 =			// 588");			// s.executeQuery("select a1, a2 from test4 where a1 = 978 and a2 =			// 588");			// s.executeQuery("select a1, a2 from test5 where a1 = 978 and a2 =			// 588");			/**			 * These queries are for doing joins across the tables			 */			// s.executeQuery("select a1, a2 from test1, test5 where a1 = a1");			// s.executeQuery("select a1, a2 from test2, test5 where a1 = a1");			// s.executeQuery("select a1, a2 from test4, test5 where a1 = a1");			conn.close();		} catch (		SQLException e)		{			e.printStackTrace();		} finally		{			try {				conn.close();			} catch (SQLException e) {				e.printStackTrace();			}		}	}}