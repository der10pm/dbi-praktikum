package dbi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AccountThread extends Thread {
	private Connection conn;
	private int n, skip, take;
	
	AccountThread(int n, int skip, int take) {
		this.n = n;
		this.skip = skip;
		this.take = take;
		try {
			conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			conn.setAutoCommit(false);
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		System.out.println("Inserting accounts " + skip + " bis " + (skip + take - 1));
		try {
			PreparedStatement stmtAcc = conn.prepareStatement(
					"insert into accounts " +
					"(accid, name, balance, branchid, address) " +
					"values(?, 'cfQEzCrLSxlnGhlRXKrS', 0, ?, 'BNZh6jqiJuXf2AkggJhCmYOLH4otgKiDqdaOf5olhX57AzQHuGTl39VCEOuYbTbUv59U')"
					);
			
			long startT = System.currentTimeMillis();
			for(int i = skip; i < skip + take; i++) {
				stmtAcc.setInt(1, i);
				stmtAcc.setInt(2, (int)(Math.random() * n + 1));
				stmtAcc.addBatch();
			}
			stmtAcc.executeBatch();
			conn.commit();
			conn.close();
			System.out.println("End (in " + (System.currentTimeMillis() - startT) + "ms)");		
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
