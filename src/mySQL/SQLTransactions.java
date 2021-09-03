package mySQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLTransactions {
	public static void main(String[] args) {

		Connection localCon1 = null;
		try {
			localCon1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "Darshil@12345");
			System.out.println("Connection 1 Started");
			localCon1.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Connection localCon2 = null;
		try {
			localCon2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "Darshil@12345");
			System.out.println("Connection 2 Started");
			localCon2.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Connection localCon3 = null;
		try {
			localCon3 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "Darshil@12345");
			System.out.println("Connection 3 Started");
			localCon3.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		PreparedStatement localPS = null;
		System.out.println("------------");
		System.out.println("Start Transaction T1");
		System.out.println("------------");

		String query = "select * from olist_customers_dataset where customer_zip_code_prefix=01151;";
		try {
			localPS = localCon1.prepareStatement(query);
			ResultSet rs = localPS.executeQuery();
			rs.next();
			System.out.println("Read Customer Data " + rs.getString("customer_id"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("------------");
		System.out.println("Start Transaction T2");
		System.out.println("------------");
		localPS = null;
		query = "select * from olist_customers_dataset where customer_zip_code_prefix=01151;";
		try {
			localPS = localCon2.prepareStatement(query);
			ResultSet rs = localPS.executeQuery();
			rs.next();
			System.out.println("Read Customer Data" + rs.getString("customer_id"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		query = "update olist_customers_dataset set customer_city = \"T1 City\" where customer_id=\"1a1aca3aea66ecc5fa26e1994aad30d6\";";
		try {
			Statement localS = localCon1.createStatement();
			int remoteRs = localS.executeUpdate(query);
			System.out.println("Updated Customers Table" + remoteRs + " rows updated");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("------------");
		System.out.println("Start Transaction T3");
		System.out.println("------------");
		localPS = null;
		query = "select * from olist_customers_dataset where customer_zip_code_prefix=01151;";
		try {
			localPS = localCon3.prepareStatement(query);
			ResultSet rs = localPS.executeQuery();
			rs.next();
			System.out.println("Read Customer Data" + rs.getString("customer_id"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		OBSERVATIONS

//		Here, this Transaction will not be performed as the first update operation is holding its lock on the same table in the database
//		To execute the next update transaction, the first transaction has to get committed in the database and release the lock from the table
//		This concurrency problem can be solved by using Threads, but the same problem persists to release lock in order to perform the transaction 
//		Locking Mechanism has to be used to maintain the consistency in the database
		
		

		query = "update olist_customers_dataset set customer_city = \"T2 City\" where customer_id=\"1a1aca3aea66ecc5fa26e1994aad30d6\";";
		try {
			Statement localS = localCon2.createStatement();
			int remoteRs = localS.executeUpdate(query);
			System.out.println("updateee 2nd   " + remoteRs + " rows updated");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		query = "update olist_customers_dataset set customer_city = \"T3 City\" where customer_id=\"1a1aca3aea66ecc5fa26e1994aad30d6\";";
		try {
			Statement localS = localCon3.createStatement();
			int remoteRs = localS.executeUpdate(query);
			System.out.println("updateee 3rd   " + remoteRs + " rows updated");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			localCon1.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			localCon3.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			localCon2.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		System.out.println("Successfully Commit");

	}
}
