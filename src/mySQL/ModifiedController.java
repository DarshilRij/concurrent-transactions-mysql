package mySQL;

import java.sql.*;

public class ModifiedController {
	public static void main(String[] args) {

		try {
			Connection localCon = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root",
					"Darshil@12345");
			System.out.println("Local Database Connected");

			Connection remoteCon = DriverManager.getConnection("jdbc:mysql://35.184.212.248:3306/Data5408", "root",
					"Darshil@12345");
			System.out.print("Remote Database Connected");

			String insertQ, updateQ, deleteQ;
			
			System.out.println("Distributed Transactions : \n");

			// TRANSACTION 1

			System.out.println("------------");
			System.out.println("\nTransaction 1\n");
			System.out.println("------------");
//			Update operations at LOCAL Site
			updateQ = "update olist_customers_dataset set customer_city = \"Toronto\" where customer_id=\"1a1aca3aea66ecc5fa26e1994aad30d6\";";
			try {
				Statement localS = localCon.createStatement();
				int localRs = localS.executeUpdate(updateQ);
				System.out.println("\nUpdated Customers Table \n" + localRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			updateQ = "update olist_geolocation_dataset set geolocation_city = \"Montreal\" where geolocation_zip_code_prefix=1333;";
			try {
				Statement localS = localCon.createStatement();
				int localRs = localS.executeUpdate(updateQ);
				System.out.println("\nUpdated Geolocation Table \n" + localRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Data Committed at Local Site");

//			Operations at REMOTE Site
			insertQ = "INSERT INTO olist_order_payments_dataset ( order_id,payment_sequential,payment_type,payment_installments,payment_value)\r\n"
					+ "   VALUES\r\n" + "   ( \"b81ef226f3fe1789b1e8b2acac839d16\",1,\"credit_card\",9,89.2);";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(insertQ);
				System.out.println("\nUpdated Geolocation Table \n" + remoteRs + " row Inserted");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			updateQ = "update olist_order_items_dataset set price = 101.11 where order_id=\"000229ec398224ef6ca0657da4fc703e\";";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(updateQ);
				System.out.println("\nUpdated Geolocation Table \n" + remoteRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			deleteQ = "delete from olist_order_items_dataset where order_id=\"000229ec398224ef6ca0657da4fc703e\";";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(updateQ);
				System.out.println("\nDeleted Geolocation Table \n" + remoteRs + " rows deleted");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Data Committed at Remote Site");

			
			
			
			// TRANSACTION 2
			System.out.println("------------");
			System.out.println("\nTransaction 2\n");
			System.out.println("------------");
//			Update operations at LOCAL Site
			updateQ = "update olist_customers_dataset set customer_city = \"Vancouver\" where customer_id=\"4a0e66fd30684aa1409cd1b66fec77cc\";";
			try {
				Statement localS = localCon.createStatement();
				int localRs = localS.executeUpdate(updateQ);
				System.out.println("\nUpdated Customers Table \n" + localRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			updateQ = "update olist_geolocation_dataset set geolocation_city = \"Sydney\" where geolocation_zip_code_prefix=1332;";
			try {
				Statement localS = localCon.createStatement();
				int localRs = localS.executeUpdate(updateQ);
				System.out.println("\nUpdated Geolocation Table \n" + localRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Data Committed at Local Site");

//			Operations at REMOTE Site
			insertQ = "INSERT INTO olist_order_payments_dataset ( order_id,payment_sequential,payment_type,payment_installments,payment_value)\r\n"
					+ "   VALUES\r\n" + "   ( \"001c85b5f68d2be0cb0797afc459ce9a\",1,\"credit_card\",7,69.2);";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(insertQ);
				System.out.println("\nUpdated Geolocation Table \n" + remoteRs + " row Inserted");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			updateQ = "update olist_order_items_dataset set price = 29.11 where order_id=\"001c85b5f68d2be0cb0797afc9e8ce9a\";";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(updateQ);
				System.out.println("\nUpdated Geolocation Table \n" + remoteRs + " rows updated");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			deleteQ = "delete from olist_order_items_dataset where order_id=\"001c85b5f68d2be0cb0797afc9e8ce9a\";";
			try {
				Statement remoteS = remoteCon.createStatement();
				int remoteRs = remoteS.executeUpdate(updateQ);
				System.out.println("\nDeleted Geolocation Table \n" + remoteRs + " rows deleted");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Data Committed at Remote Site");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
