package recommend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeopleMatrix {

	private static Map<Long, Map<Long, Integer>> peopleMatrix;
	private static Counter counter = new Counter();
	int n = 3;

	public Map<Long, Map<Long, Integer>> createPeopleMatrix() throws Exception {
		peopleMatrix = new ConcurrentHashMap<Long, Map<Long, Integer>>();
		String members = "select id  from member limit 0,10";
		String consultation_sql = "select  member,goods from  consultation con  where con.member!=0 ";
		String member_favorite_goods_sql = "select  favorite_members,favorite_goods from member_favorite_goods mfg  where mfg.favorite_members!=0 ";
		String good_sql = "select  o.member,p.goods from orders o left join order_item oi on oi.orders=o.id left join product p on p.id=oi.product where o.member!=0";
		String review_sql = "select re.member,re.goods,re.score  from review re where re.member!=0";

		Connection conn = SqlUtil.getConnection();
		Thread t1 = new Thread(new Step5(conn, members));
		t1.start();
		synchronized (counter) {
			while (counter.getCounter() < 0)
				counter.wait();
		}
		new Thread(new Step6(conn, consultation_sql, 4)).start();
		new Thread(new Step6(conn, good_sql, 3)).start();
		new Thread(new Step6(conn, review_sql)).start();
		synchronized (counter) {
			while (counter.getCounter() < n)
				counter.wait();
		}
		return peopleMatrix;
	}

	public static Map<Long, Map<Long, Integer>> getPeopleMatrix() {
		return peopleMatrix;
	}

	class Step5 implements Runnable {

		String sql;
		Connection conn;

		public Step5(Connection conn, String sql) {
			this.sql = sql;
			this.conn = conn;
		}

		@Override
		public void run() {
			ResultSet rs = null;
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					peopleMatrix.put(rs.getLong(1), new HashMap<Long, Integer>());
				}
				synchronized (counter) {
					counter.increaseCounter();
					counter.notify();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}

	}

	class Step6 implements Runnable {

		String sql;
		int score;
		Connection conn;

		public Step6(Connection conn, String sql, int score) {
			this.sql = sql;
			this.score = score;
			this.conn = conn;
		}

		public Step6(Connection conn, String sql) {
			this.sql = sql;
			this.conn = conn;
		}

		@Override
		public void run() {
			Statement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				int columnCount = rs.getMetaData().getColumnCount();
				if (columnCount == 2) {
					while (rs.next()) {
						synchronized (peopleMatrix) {
							Map<Long, Integer> map = null;
							map = peopleMatrix.get(rs.getLong(1));
							long good = rs.getLong(2);
							if (map != null && map.containsKey(good)) {
								map.put(good, map.get(good) + score);
							} else if (map != null) {
								map.put(good, score);
							}
						}

					}
				} else {
					while (rs.next()) {
						Map<Long, Integer> map = peopleMatrix.get(rs.getLong(1));
						long good = rs.getLong(2);
						synchronized (peopleMatrix) {
							if (map != null && map.containsKey(good)) {
								map.put(good, map.get(good) + rs.getInt(3));
							} else if (map != null) {
								map.put(good, rs.getInt(3));
							}
						}

					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					stmt.close();
					synchronized (counter) {
						counter.increaseCounter();
						;
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				synchronized (counter) {
					if (counter.getCounter() == n) {
						SqlUtil.close();
						counter.notify();
					}
				}
			}
			System.out.println(Thread.currentThread().getName() + "over");
		}
	}
}
