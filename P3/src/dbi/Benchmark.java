package dbi;
import java.sql.*;
import java.util.ArrayList;

public class Benchmark {
	Connection conn;
	int n, threadCount;
	String connStrg;
	
	/**
	 * Hauptklasse für den Benchmark
	 * @param n Größe der Datenbank
	 * @param threadCount Anzahl der zu nutzenden Threads für das Erstellen der Accountdaten
	 * @param connStrg ConnectionString
	 */
	Benchmark(int n, int threadCount, String connStrg) throws SQLException {
		conn = DriverManager.getConnection(connStrg, "postgres", "daten1");
		conn.setAutoCommit(false);
		this.n = n;
		this.threadCount = threadCount;
		this.connStrg = connStrg;
	}
	
	/**
	 * Erstellen der Datenbank
	 * Einfügen von Branches, Accounts und Tellers
	 */
	public void initDB() {
		try {
			insertBranches();
			conn.commit();
			insertAccounts();
			insertTellers();
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Leeren der Datenbank
	 * Tables werden komplett gedroppt und neu erstellt, da dies schneller ist als die Inalte zu löschen
	 */
	public void clearDB() throws SQLException {
		try {
			PreparedStatement stmt = conn.prepareStatement(
					"DROP TABLE history;" +
					"DROP TABLE accounts;" + 
					"DROP TABLE tellers;" + 
					"DROP TABLE branches;" +
					"create table branches\n" + 
					"( branchid int not null,\n" + 
					"branchname char(20) not null,\n" + 
					"balance int not null,\n" + 
					"address char(72) not null,\n" + 
					"primary key (branchid) );\n" + 
					"\n" + 
					"create table accounts\n" + 
					"( accid int not null,\n" + 
					"name char(20) not null,\n" + 
					"balance int not null,\n" + 
					"branchid int not null,\n" + 
					"address char(68) not null,\n" + 
					"primary key (accid),\n" + 
					"foreign key (branchid) references branches );\n" + 
					"\n" + 
					"create table tellers\n" + 
					"( tellerid int not null,\n" + 
					"tellername char(20) not null,\n" + 
					"balance int not null,\n" + 
					"branchid int not null,\n" + 
					"address char(68) not null,\n" + 
					"primary key (tellerid),\n" + 
					"foreign key (branchid) references branches );\n" + 
					"\n" + 
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
		} catch (SQLException e) {
			e.printStackTrace();
			conn.rollback();
		}
	}
	
	/**
	 * n Branches werden eingefügt
	 */
	public void insertBranches() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
				"insert into branches" +
				"(branchid, balance, branchname, address) " +
				"values(?,0,'cfQEzCrLSxlnGhlRXKrS','4pMOdU3kxqBorIP6wHkerWp2yuGJSWDP7ZCnSYqOlceivFNmynljwOdyNcl3s8Le8zXp3ryz')");
		
		
		for(int i = 1; i <= n; i++) {
			stmt.setInt(1, i);
			stmt.addBatch();
		}
		stmt.executeBatch();
	}
	
	/**
	 * n * 100000 Accounts werden eingefügt, dazu werden mehrere Threads verwendet um die Last aufzuteilen
	 */
	public void insertAccounts() throws SQLException {
		int count = n * 100000;
		if (threadCount == 1) {					// Wenn nur 1 Thread genutzt werden soll, wird der Hauptthread verwendet
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
		} else if (threadCount <= count) {		// Bei mehreren Threads wird die Last aufgeteilt und alle Threads ausgeführt
			int skip = count / threadCount;
			ArrayList<AccountThread> threads = new ArrayList<>(); 
			for (int i = 1; i <= count; i += skip) {
				AccountThread th = new AccountThread(connStrg, n, i, ((skip + i - 1) > count ? skip - ((skip + i - 1) - count) : skip));
				th.start();
				threads.add(th);
			}
			// Warten auf die Threads
			for (AccountThread th : threads) {
				try {
					th.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new SQLException(e.getCause());
				}
			}
		} else {
			System.out.println("Too many threads for too little records");
		}
	}
	
	/**
	 * Erstellen der Daten in der Teller Tabelle
	 */
	public void insertTellers() throws SQLException {
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
	}
	
	/**
	 * Main Methode
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
		Benchmark bench = new Benchmark(10, 10, "jdbc:postgresql://localhost/benchmark?reWriteBatchedInserts=true");
		bench.clearDB();
		System.out.println("DB cleared!");
		long startTime = System.nanoTime();
		bench.initDB();
		long estimatedTime = System.nanoTime() - startTime;
		System.out.println("Fertig nach " + estimatedTime/1000000 + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}		
	}
}
