package dbi;

import java.sql.*;

public class TX extends Thread {
	Connection conn;
	
	public TX(String connStrg) throws SQLException {
		conn = DriverManager.getConnection(connStrg, "postgres", "daten1");
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		conn.setAutoCommit(false);
	}
	
	public int selectAccountBalance(int accId) throws SQLException {
		int trys = 0;
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
				PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM accounts WHERE accId=?");
				stmt.setInt(1, accId);
				ResultSet rs = stmt.executeQuery();
				rs.next();
				conn.endRequest();
				return rs.getInt("balance");
			} catch (Exception e) {
				System.out.println("retry selectAccountBalance: " + ++trys);
				conn.rollback();
			}
		}
	}

	public void insertMoney(int accId, int tellerId, int branchId, int delta) throws SQLException {
		int trys = 0;
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
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
				stmt.execute();
				conn.commit();
				conn.endRequest();
				return;
			} catch (Exception e) {
				System.out.println("rolling back insert:" + ++trys);
				conn.rollback();
			}
		}
	}
	
	public int analyse(int delta) throws SQLException {
		int trys = 0;
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
				PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(delta) AS anzahl FROM history WHERE delta=?");
				stmt.setInt(1, delta);
				ResultSet rs = stmt.executeQuery();
				rs.next();
				conn.endRequest();
				return rs.getInt("anzahl");
			} catch (Exception e) {
				System.out.println("retry analyse: " + ++trys);
				conn.rollback();
			}
		}
	}
	
	public void run() {
		System.out.println("Thread gestartet!");
		try {
			long startTime = System.currentTimeMillis();
			long count = 0;
			while ((System.currentTimeMillis() - startTime) < 600000) {
				int random = (int) (Math.random() * 20);
				if (random < 7) {
					selectAccountBalance(((int) Math.random() * 10000000) + 1);
				} else if (random >= 7 && random < 17) {
					insertMoney(
							((int) (Math.random() * 10000000)) + 1,
							((int) (Math.random() * 1000)) + 1,
							((int) (Math.random() * 100)) + 1,
							((int) (Math.random() * 10000)) + 1);
				} else {
					analyse(((int) (Math.random() * 10000)) + 1);
				}
				if ((System.currentTimeMillis() - startTime) > 240000 && (System.currentTimeMillis() - startTime) < 540000)
					count++;
				Thread.sleep(50);
			}
			System.out.println("Count: " + count + ", Durchschnitt: " + count / 300D);
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
