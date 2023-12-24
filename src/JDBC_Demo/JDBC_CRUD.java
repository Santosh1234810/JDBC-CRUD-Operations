package JDBC_Demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class JDBC_CRUD {
	
	Connection con;
	Statement statement;
	ResultSet resultSet;
	
	public Connection makeConnection() throws SQLException {
		
			con = DriverManager.getConnection("jdbc:mysql://localhost/upi_payment","root","root");
			
			if (con == null) {
				System.out.println("Connection Not Established");
			}
			else {
				System.out.println("Connection Established!!!");
			}
			return con;
	}
	public void retrive() throws SQLException{
		Statement stmt = con.createStatement();
		String s1 = "select * from students";
		ResultSet rs = stmt.executeQuery(s1);
		
		while (rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			double marks = rs.getDouble("marks");
			
			System.out.println("Id: "+id +" Name: " +name +" Age: " + age+" Marks: "+marks);
		}
	}
	
	public void insert() throws SQLException {
//		insert the records 
		
		Statement stmt = con.createStatement();
		String s2 = String.format("insert into students(name,age,marks) values('%s',%o,%f)","Rahul",25,75.7);
		int rowsAffected = stmt.executeUpdate(s2);
		
		if (rowsAffected > 0) {
			System.out.println("Data Inserted Successfully...");
		}
		else {
			System.out.println("Data not inserted...");
		}
	}
	
	public void update() throws SQLException {
//		update

//		statement = con.createStatement();
		makeConnection();
		String s3 = String.format("update students set marks = %f where id = %d",85.5,14);
		
		int rowsUpdated = statement.executeUpdate(s3);
		
		if (rowsUpdated > 0) {
			System.out.println("Data Updated Succesfully...");
		}
		else {
			System.out.println("Data Not Updated...");
		}
		
	}
	
	public void delete() throws SQLException {
		statement = con.createStatement();
		String s4 = String.format("delete from students where id = 7");
		int rowsDeleted = statement.executeUpdate(s4);
		
		if (rowsDeleted > 0) {
			System.out.println("Record Deleted Successfully...");
		}
		else {
			System.out.println("Data Not Deleted...");
		}
	}
	
	public void insert2() throws SQLException {
//		CRUD Operation using PreaparedStatement
//		Insert the data
		statement = con.createStatement();
		String s5 = "insert into students(name, age, marks) values(?, ?, ?)";
		PreparedStatement preparedStatement = con.prepareStatement(s5);
		preparedStatement.setString(1, "Amruta");
		preparedStatement.setInt(2, 24);
		preparedStatement.setDouble(3, 65.6);
		int rowsAffeted = preparedStatement.executeUpdate();
		
		if (rowsAffeted > 0) {
			System.out.println("Data Inserted Successfully...");
		}
		else {
			System.out.println("Data Not Inserted...");
		}
	}
	
	public void display() throws SQLException {
//		Retrieve the data using PreparedStatement
		String s6 = "Select * from students";
		PreparedStatement preparedStatement2 = con.prepareStatement(s6);
		statement = con.createStatement();
		ResultSet resultSet = preparedStatement2.executeQuery();
		
		while (resultSet.next()) {
			int id = resultSet.getInt("id");
			String name = resultSet.getString("name");
			int age = resultSet.getInt("age");
			double marks = resultSet.getDouble("marks");
			
			System.out.println("Id: "+id +" "+"Name: "+name +" "+"Age: "+age +" "+"Marks: "+marks);
		}
	}
	
	public void update2() throws SQLException {
//		Update using PreparedStatement
		String query = "update students set marks = ? where id = ?";
		PreparedStatement preparedStatement = con.prepareStatement(query);
		preparedStatement.setDouble(1, 87.7);
		preparedStatement.setInt(2, 8);
		int recordUpdated = preparedStatement.executeUpdate();
		
		if (recordUpdated > 0) {
			System.out.println("Data Updated Successfully...");
		}
		else {
			System.out.println("Data Not Updated...");
		}
	}
	
	public void delete2() throws SQLException{
//		Delete using PreparedStatement
		String query2 = "delete from students where id = ?";
		PreparedStatement preparedStatement2 = con.prepareStatement(query2);
		preparedStatement2.setInt(1, 10);
		int recordDeleted = preparedStatement2.executeUpdate();
		
		if (recordDeleted > 0) {
			System.out.println("Data Deleted Successfully...");
		}
		else {
			System.out.println("Data Not Deleted...");
		}
	}
	
	public void batchProcessing() throws SQLException {
//		Batch Processing Using Statement Interface
		
		Statement stmt = con.createStatement();
		Scanner sc = new Scanner(System.in);
		
		while (true) {
			System.out.print("Enter the Name: ");
			String name = sc.next();
			System.out.print("Enter the Age: ");
			int age = sc.nextInt();
			System.out.print("Enter the Marks: ");
			double marks = sc.nextDouble();
			
			System.out.print("Do you want to enter more data(Y,N)?: ");
			String choice = sc.next();
			
			String query3 = String.format("insert into students(name, age, marks) values('%s', %d, %f)",name, age, marks);
			stmt.addBatch(query3);
			
			if (choice.toUpperCase().equals("N")) {
				break;
			}
		}
		
		int arr[] = stmt.executeBatch();
		
		for (int i=0; i<arr.length; i++) {
			if (arr[i] == 0) {
				System.out.println("Query "+i +" not executed successfully!!!");
			}
		}
	}
	
	public void batchProcessing2() throws SQLException {
//		Batch Processing using Prepared Statement Interface
		String q = "insert into students(name, age, marks) values(?, ?, ?)";
		PreparedStatement preparedStatement = con.prepareStatement(q);
		try (Scanner scanner = new Scanner(System.in)) {
			while(true) {
				System.out.print("Enter the Name: ");
				String name = scanner.next();
				System.out.print("Enter the Age: ");
				int age = scanner.nextInt();
				System.out.print("Enter the Marks: ");
				double marks = scanner.nextDouble();
				
				System.out.print("Do you want insert more data?(Y/N): ");
				String choice = scanner.next();
				
				preparedStatement.setString(1, name);
				preparedStatement.setInt(2, age);
				preparedStatement.setDouble(3, marks);
				
				preparedStatement.addBatch();
				
				if (choice.toUpperCase().equals("N")) {
					break;
				}
			}
		}
		
		int arr2[] = preparedStatement.executeBatch();
		
		for (int i=0; i<arr2.length; i++) {
			if (arr2[i] == 0) {
				System.out.println("Query "+i +" not executed successfully!!!");
			}
		}
	}
	
	public void transactionManagement() throws SQLException{
//		Transaction Management
		con.setAutoCommit(false);
		String debit_query = "update account set balance = balance - ? where account_number = ?";
		String credit_query = "update account set balance = balance + ? where account_number = ?";
		PreparedStatement debitPreaparedStatement = con.prepareStatement(debit_query);
		PreparedStatement creditPreparedStatement = con.prepareStatement(credit_query);
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the Account Number: ");
		int account_number = sc.nextInt();
		System.out.print("Enter the Amount: ");
		double amount = sc.nextDouble();
		debitPreaparedStatement.setDouble(1, amount);
		debitPreaparedStatement.setInt(2, account_number);
		creditPreparedStatement.setDouble(1, amount);
		creditPreparedStatement.setInt(2, 102);
		
		debitPreaparedStatement.executeUpdate();
		creditPreparedStatement.executeUpdate();
		if (isSufficient(con, account_number, amount)) {
			con.commit();
			System.out.println("Transaction Successfull!!!");
		}
		else {
			con.rollback();
			System.out.println("Transaction Failed!!!");
		}
	}
	
static boolean isSufficient(Connection connection, int account_number, double amount) {
	try {
		String query = "select balance from account where account_number = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		preparedStatement.setInt(1, account_number);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		if (resultSet.next()) {
			double current_balance = resultSet.getDouble("balance");
			
			if (amount > current_balance) {
				return false;
			}
			else {
				return true;
			}
		}
	}
	catch (SQLException ex) {
		System.out.println(ex.getMessage());
	}
	return false;
}
 	public static void main(String[] args) throws SQLException{
		JDBC_CRUD tpt = new JDBC_CRUD();
		tpt.makeConnection();
		tpt.insert();
		tpt.update();
		tpt.retrive();
		tpt.delete();
		tpt.insert2();
		tpt.display();
		tpt.update2();
		tpt.delete2();
		tpt.batchProcessing();
		tpt.batchProcessing2();
		tpt.transactionManagement();
 	}
}