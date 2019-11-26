import java.sql.*;

public class DBIConnection {

	public static void main(String[] args) {
		try {
			long startTime = System.nanoTime();
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			conn.setAutoCommit(false);
			int n = 10;
			PreparedStatement stmt = conn.prepareStatement(
					"insert into branches" +
					"(branchid, balance, branchname, address) " +
					"values(?,0,'cfQEzCrLSxlnGhlRXKrS','4pMOdU3kxqBorIP6wHkerWp2yuGJSWDP7ZCnSYqOlceivFNmynljwOdyNcl3s8Le8zXp3ryz')");
			
			
			for(int i = 1; i <= n; i++) {
				stmt.setInt(1, i);
				stmt.addBatch();
				}
			stmt.executeBatch();
			
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
			long estimatedTime = System.nanoTime() - startTime;
			System.out.println("Fertig nach " + estimatedTime/1000000 + " ms");
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
