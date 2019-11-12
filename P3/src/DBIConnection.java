import java.sql.*;

public class DBIConnection {

	public static void main(String[] args) {
		try {
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/CAP-Vertriebsdatenbank", "postgres", "daten1");
			System.out.println(conn);
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
