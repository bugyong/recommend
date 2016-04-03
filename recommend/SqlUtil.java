package recommend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlUtil {
	private static Connection conn;

	// private static Statement stmt;
	// private static ResultSet rs;
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/wShop", "root", "root");
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static Connection getConnection() throws Exception {
		// Class.forName("com.mysql.jdbc.Driver");
		// java.sql.Connection conn =
		// DriverManager.getConnection("jdbc:mysql://localhost/wShop", "root",
		// "root");

		if (conn.isClosed()) {
			conn = DriverManager.getConnection("jdbc:mysql://localhost/wShop", "root", "root");
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		}

		return conn;
		// rs = stmt.executeQuery(sql);
		// return rs;
	}

	public static boolean close() {
		try {
			// rs.close();
			// stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
