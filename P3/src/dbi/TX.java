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
		long startTime = System.currentTimeMillis();
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
				PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM accounts WHERE accId=?");
				stmt.setInt(1, accId);
				ResultSet rs = stmt.executeQuery();
				rs.next();
				conn.endRequest();
				System.out.println("select: \t" + (System.currentTimeMillis() - startTime));
				return rs.getInt("balance");
			} catch (Exception e) {
				conn.rollback();
			}
		}
	}

	public void insertMoney(int accId, int tellerId, int branchId, int delta) throws SQLException {
		long startTime = System.currentTimeMillis();
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
				CallableStatement stmt = conn.prepareCall(
						"call insert_transaction(?, ?, ?, ?, ?)");
				stmt.setInt(1, accId);
				stmt.setInt(2, branchId);
				stmt.setInt(3, tellerId);
				stmt.setInt(4, delta);
				stmt.setString(5, "abcdefghijklmnopqrstuvwxyzabcd");
				stmt.execute();
				conn.commit();
				conn.endRequest();
				System.out.println("insert: \t" + (System.currentTimeMillis() - startTime));
				return;
			} catch (Exception e) {
				conn.rollback();
			}
		}
	}
	
	public int analyse(int delta) throws SQLException {
		long startTime = System.currentTimeMillis();
		while (true) {
			try {
				conn.endRequest();
				conn.beginRequest();
				PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(delta) AS anzahl FROM history WHERE delta=?");
				stmt.setInt(1, delta);
				ResultSet rs = stmt.executeQuery();
				rs.next();
				conn.endRequest();
				System.out.println("analyse: \t" + (System.currentTimeMillis() - startTime));
				return rs.getInt("anzahl");
			} catch (Exception e) {
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
