package dbi;

import java.sql.*;
import java.util.ArrayList;

public class TX extends Thread {
	Connection conn;
	
	/**
	 * Erzeugt einen neuen Thread für den Load test
	 * @param connStrg Connection string der Datenbankverbindung
	 * @throws SQLException
	 */
	public TX(String connStrg) throws SQLException {
		conn = DriverManager.getConnection(connStrg, "postgres", "daten1");
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		conn.setAutoCommit(false);
	}
	
	/**
	 * Führt eine Transaktion aus, bei der das Guthaben eines Accounts zurück gegeben wird
	 * @param accId Id des Accounts
	 * @return Guthaben des Accounts
	 * @throws SQLException
	 */
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
				conn.commit();
				conn.endRequest();
				System.out.println("select: \t" + (System.currentTimeMillis() - startTime));
				return rs.getInt("balance");
			} catch (Exception e) {
				conn.rollback();
			}
		}
	}

	/**
	 * Führt eine Transaktion aus, bei der eine Einzahlung durchgeführt wird. 
	 * Dazu wird beim Account, beim Branch und beim Teller das delta auf den Kontostand addiert 
	 * und ein Eintrag in der History Tabelle erzeugt
	 * 
	 * @param accId Id des Accounts
	 * @param tellerId Id des Tellers
	 * @param branchId Id des Branches
	 * @param delta delta der Einzahlung
	 * @throws SQLException
	 */
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
	
	/**
	 * Führt eine Transaktion aus bei der abgefragt wird, wie viele Einzahlungen es mit delta gegeben hat
	 * @param delta Delta der Einzahlung
	 * @return Anzahl der Einzahlungen
	 * @throws SQLException
	 */
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
				conn.commit();
				conn.endRequest();
				System.out.println("analyse: \t" + (System.currentTimeMillis() - startTime));
				return rs.getInt("anzahl");
			} catch (Exception e) {
				conn.rollback();
			}
		}
	}
	
	/**
	 * Wird beim Starten des Threads ausgeführt.
	 * Lässt für 10 Minuten eine Schleife laufen, in der zufällig eine von Transaktionen ausgeführt wird.
	 * Zwischenzeitlich wird für 5 Minuten gemessen, wie viele Transaktionen erfolgen.
	 */
	public void run() {
		System.out.println("Thread gestartet!");
		try {
			long startTime = System.currentTimeMillis();
			long count = 0;
			while ((System.currentTimeMillis() - startTime) < 600000) {
				int random = (int) (Math.random() * 20);
				if (random < 7) {							// 0 bis 6 für Transaktion 1
					selectAccountBalance(((int) Math.random() * 10000000) + 1);
				} else if (random >= 7 && random < 17) {	// 7 bis 16 für Transaktion 2
					insertMoney(
							((int) (Math.random() * 10000000)) + 1,
							((int) (Math.random() * 1000)) + 1,
							((int) (Math.random() * 100)) + 1,
							((int) (Math.random() * 10000)) + 1);
				} else {									// 17 bis 19 für Transaktion 3
					analyse(((int) (Math.random() * 10000)) + 1);
				}
				// Misst alle erfolgreichen Transaktionen im Zeitfenster von 4 bis 9 Minuten
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
	
	/**
	 * Main Methode des Load Driver Programms
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// Leeren der history Tabelle
			Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.122.9:5432/benchmark?reWriteBatchedInserts=true", "postgres", "daten1");
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
			conn.close();
			
			// Ausführen der Transaktionsthreads
			ArrayList<Thread> threads = new ArrayList<>();
			for (int i = 0; i < 5; i++) {
				TX transactions = new TX("jdbc:postgresql://192.168.122.9:5432/benchmark?reWriteBatchedInserts=true");
				threads.add(transactions);
				transactions.start();
			}
			// Warten auf das Beenden der Threads
			for (Thread th : threads) {
				th.join();
			}
		} catch (Exception e) {
			// Wenn hier ein Fehler landet wird as Programm abgebrochen.
			e.printStackTrace();
		}
	}
}
