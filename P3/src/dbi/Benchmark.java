package dbi;
import java.sql.*;

public class Benchmark {
	Connection conn;
	int n;
	
	Benchmark(int n) {
		try {
		conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
		conn.setAutoCommit(false);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		this.n = n;
	}
	
	public void initDB() {
		insertBranches(conn, n);
//		try {
//			conn.commit();
//		} catch (SQLException e) {
//			System.out.println(e.getMessage());
//		}
		insertAccounts(conn, n);
		insertTellers(conn, n);
//		TellerThread th = new TellerThread(n);
//		AccountThread accTh = new AccountThread(n);
//		th.start();
//		accTh.start();
//		try {
//			th.join();
//			accTh.join();
//		} catch (InterruptedException e) {
//			System.out.println(e.getMessage());
//		}
		try {
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void clearDB() {
		try {
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			PreparedStatement stmt = conn.prepareStatement(
					"DELETE FROM accounts;" + 
					"DELETE FROM tellers;" + 
					"DELETE FROM branches;");
			stmt.execute();
		} catch (SQLException e) {
			
		}
	}
	
	public void insertBranches(Connection conn, int n) {
		try {
			PreparedStatement stmt = conn.prepareStatement(
					"insert into branches" +
					"(branchid, balance, branchname, address) " +
					"values(?,0,'cfQEzCrLSxlnGhlRXKrS','4pMOdU3kxqBorIP6wHkerWp2yuGJSWDP7ZCnSYqOlceivFNmynljwOdyNcl3s8Le8zXp3ryz')");
			
			
			for(int i = 1; i <= n; i++) {
				stmt.setInt(1, i);
				stmt.addBatch();
				}
			stmt.executeBatch();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void insertAccounts(Connection conn, int n) {
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
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void insertTellers(Connection conn, int n) {
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
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		Benchmark bench = new Benchmark(10);

		bench.clearDB();
		System.out.println("DB cleared!");
		long startTime = System.nanoTime();
		bench.initDB();
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("Fertig nach " + estimatedTime/1000000 + " ms");
	}
}
