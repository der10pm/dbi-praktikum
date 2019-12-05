package dbi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AccountThread extends Thread {
	private Connection conn;
	private int n, start, count;
	
	/**
	 * Thread zum erstellen der Accountdaten mit eigener Connection
	 * @param n Größe der Datenbank (Anzahl der Branches)
	 * @param start Erster Eintrag dieses Threads
	 * @param count Anzahl der zu erstellenden Einträge
	 */
	AccountThread(int n, int start, int count) {
		this.n = n;
		this.start = start;
		this.count = count;
		try {
			conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
			conn.setAutoCommit(false);
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Methode wird beim Start des Threads ausgeführt
	 * Erstellt von strat ausgehend (start + count) Einträge in der Accountdatenbank
	 */
	public void run() {
		System.out.println("Inserting accounts " + start + " bis " + (start + count - 1));
		try {
			PreparedStatement stmtAcc = conn.prepareStatement(
					"insert into accounts " +
					"(accid, name, balance, branchid, address) " +
					"values(?, 'cfQEzCrLSxlnGhlRXKrS', 0, ?, 'BNZh6jqiJuXf2AkggJhCmYOLH4otgKiDqdaOf5olhX57AzQHuGTl39VCEOuYbTbUv59U')"
					);
			
			long startT = System.currentTimeMillis();
			for(int i = start; i < start + count; i++) {
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
