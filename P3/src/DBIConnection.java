import java.sql.*;

public class DBIConnection {

	public static void main(String[] args) {
		try {
			
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			
			int n = 10;
			String brName = "cfQEzCrLSxlnGhlRXKrS";
			String brAdress = "4pMOdU3kxqBorIP6wHkerWp2yuGJSWDP7ZCnSYqOlceivFNmynljwOdyNcl3s8Le8zXp3ryz";
			PreparedStatement stmt = conn.prepareStatement(
					"insert into branches" +
					"(branchid, balance, branchname, address) " +
					"values(?,0,?,?)");
			stmt.setString(2, brName);
			stmt.setString(3, brAdress);		
			
			for(int i = 1; i <= n; i++) {
				stmt.setInt(1, i);
				stmt.executeUpdate();
				}
			
			String accAddress = "BNZh6jqiJuXf2AkggJhCmYOLH4otgKiDqdaOf5olhX57AzQHuGTl39VCEOuYbTbUv59U";
			PreparedStatement stmtAcc = conn.prepareStatement(
					"insert into accounts " +
					"(accid, name, balance, branchid, address) " +
					"values(?, ?, 0, ?, ?)"
					);
			stmtAcc.setString(4, accAddress);
			stmtAcc.setString(2, brName);

			
			for(int i = 1; i <= n * 100000; i++) {
				stmtAcc.setInt(1, i);
				stmtAcc.setInt(3, (int)(Math.random() * n + 1));
				stmtAcc.executeUpdate();
			}
			
			PreparedStatement stmtTel = conn.prepareStatement(
					"insert into tellers " +
					"(tellerid, tellername, balance, branchid, address) " +
					"values(?, ?, 0, ?, ?) "
					);
			stmtTel.setString(2, brName);
			stmtTel.setString(4, accAddress);
			
			for(int i = 1; i <= n * 10; i++) {
				stmtTel.setInt(1, i);
				stmtTel.setInt(3, (int)(Math.random() * n + 1));
				stmtTel.executeUpdate();
			}			
			System.out.println("mama fertig.");
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
