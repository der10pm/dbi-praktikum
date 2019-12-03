package dbi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AccountThread extends Thread {
	private Connection conn;
	private int n;
	
	AccountThread(int n) {
		this.n = n;
		try {
			conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			conn.setAutoCommit(false);
		} catch(SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void run() {
		System.out.println("Inserting accounts");
		try {
			PreparedStatement stmtAcc = conn.prepareStatement(
					"insert into accounts " +
					"(accid, name, balance, branchid, address) " +
					"values(?, 'cfQEzCrLSxlnGhlRXKrS', 0, ?, 'BNZh6jqiJuXf2AkggJhCmYOLH4otgKiDqdaOf5olhX57AzQHuGTl39VCEOuYbTbUv59U')"
					);
			
			
			for(int i = 1; i <= n * 100000; i++) {
				stmtAcc.setInt(1, i);
				stmtAcc.setInt(2, (int)(Math.random() * n + 1));
				stmtAcc.addBatch();
			}
			stmtAcc.executeBatch();
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
}
