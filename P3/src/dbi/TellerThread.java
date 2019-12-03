package dbi;

import java.sql.*;

public class TellerThread extends Thread {
	private Connection conn;
	private int n;
	
	TellerThread(int n) {
		this.n = n;
		try {
			conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			conn.setAutoCommit(false);
		} catch(SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void run() {
		System.out.println("Inserting tellers");
		try {
			PreparedStatement stmtTel = conn.prepareStatement(
					"insert into tellers " +
					"(tellerid, tellername, balance, branchid, address) " +
					"values(?, 'cfQEzCrLSxlnGhlRXKrS', 0, ?, 'BNZh6jqiJuXf2AkggJhCmYOLH4otgKiDqdaOf5olhX57AzQHuGTl39VCEOuYbTbUv59U') "
					);
			
			for(int i = 1; i <= n * 10; i++) {
				stmtTel.setInt(1, i);
				stmtTel.setInt(2, (int)(Math.random() * n + 1));
				stmtTel.addBatch();
			}		
			stmtTel.executeBatch();
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
}
