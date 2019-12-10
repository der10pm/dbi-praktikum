package dbi;

import java.sql.*;

public class TX {
	Connection conn;
	
	public TX(String connStrg) throws SQLException {
		conn = DriverManager.getConnection(connStrg, "postgres", "daten1");
		conn.setAutoCommit(false);
	}
	
	public int selectAccountBalance(int accId) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM accounts WHERE accId=?");
		stmt.setInt(1, accId);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		return rs.getInt("balance");
	}

	public void insertMoney(int accId, int tellerId, int branchId, int delta) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
				"UPDATE accounts SET balance=balance+? WHERE accId=?;"
				+ "UPDATE tellers SET balance=balance+? WHERE tellerId=?;"
				+ "UPDATE branches SET balance=balance+? WHERE branchId=?;"
				+ "INSERT INTO history"
				+ "(accId, tellerId, delta, branchId, accBalance, cmmnt)"
				+ "VALUES"
				+ "(?, ?, ?, ?, (SELECT balance FROM accounts WHERE accId=?), ?)");
		stmt.setInt(1, delta);
		stmt.setInt(2, accId);
		stmt.setInt(3, delta);
		stmt.setInt(4, tellerId);
		stmt.setInt(5, delta);
		stmt.setInt(6, branchId);
		stmt.setInt(7, accId);
		stmt.setInt(8, tellerId);
		stmt.setInt(9, delta);
		stmt.setInt(10, branchId);
		stmt.setInt(11, accId);
		stmt.setString(12, "abcdefghijklmnopqrstuvwxyzabcd");
		try {
				stmt.execute();
				conn.commit();	
		} catch (SQLException e) {
			conn.rollback();
		}
	}
	
	public int analyse(int delta) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(delta) AS anzahl FROM history WHERE delta=?");
		stmt.setInt(1, delta);
		ResultSet rs = stmt.executeQuery();
		rs.next();
		return rs.getInt("anzahl");
	}
	
	public void clearHistory() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
				"DROP TABLE history;" +
				"create table history\n" + 
				"( accid int not null,\n" + 
				"tellerid int not null,\n" + 
				"delta int not null,\n" + 
				"branchid int not null,\n" + 
				"accbalance int not null,\n" + 
				"cmmnt char(30) not null,\n" + 
				"foreign key (accid) references accounts,\n" + 
				"foreign key (tellerid) references tellers,\n" + 
				"foreign key (branchid) references branches );"
				);
		stmt.execute();
		conn.commit();
	}
}
