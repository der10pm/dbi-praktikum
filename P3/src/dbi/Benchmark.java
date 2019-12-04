package dbi;
import java.sql.*;
import java.util.ArrayList;

public class Benchmark {
	Connection conn;
	int n;
	
	Benchmark(int n) {
		try {
		conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
		conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.n = n;
	}
	
	public void initDB() {
		insertBranches(conn, n);
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		insertAccounts(conn, n);
		insertTellers(conn, n);
		try {
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void clearDB() {
		try {
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/benchmark", "postgres", "daten1");
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
		} catch (SQLException e) {
			e.printStackTrace();
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
			e.printStackTrace();
		}
	}
	
	public void insertAccounts(Connection conn, int n) {
		int threadCount = 4;
		int count = n * 100000;
		if (threadCount == 1) {
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
		} else if (threadCount <= count) {
			int skip = count / threadCount;
			ArrayList<AccountThread> threads = new ArrayList<>(); 
			for (int i = 1; i <= count; i += skip) {
				AccountThread th = new AccountThread(n, i, ((skip + i - 1) > count ? skip - ((skip + i - 1) - count) : skip));
				th.start();
				threads.add(th);
			}
			for (AccountThread th : threads) {
				try {
					th.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("Too many threads for too little records");
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
			e.printStackTrace();
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
