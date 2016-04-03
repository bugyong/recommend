package recommend;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RateMatrix {

	private static Map<Long, Map<Long, Integer>> rateMatrix;
	private static Counter counter = new Counter();

	public Map<Long, Map<Long, Integer>> createRateMatrix() throws Exception {
		rateMatrix = new ConcurrentHashMap<Long, Map<Long, Integer>>();
		String goods = "select goods.id  from dx_goods goods limit 0,100";

		String consultation_sql = "select goods , member from  consultation con  where con.member!=0 ";
		String member_favorite_goods_sql = "select favorite_goods , favorite_members from member_favorite_goods mfg  where mfg.favorite_members!=0 ";
		String good_sql = "select p.goods, o.member from orders o left join order_item oi on oi.orders=o.id left join product p on p.id=oi.product where o.member!=0";
		String review_sql = "select re.goods ,re.member,re.score  from review re where re.member!=0";

		Connection conn = SqlUtil.getConnection();
		Thread t1 = new Thread(new Step1(conn, goods));
		t1.start();
		synchronized (counter) {
			while (counter.getCounter() < 0)
				counter.wait();
		}

		new Thread(new Step2(conn, consultation_sql, 4)).start();
		new Thread(new Step2(conn, member_favorite_goods_sql, 5)).start();
		new Thread(new Step2(conn, good_sql, 3)).start();
		new Thread(new Step2(conn, review_sql)).start();
		synchronized (counter) {
			while (counter.getCounter() < 4)
				counter.wait();
		}
		return rateMatrix;
	}

	public static Map<Long, Map<Long, Integer>> getRateMatrix() {
		return rateMatrix;
	}

	class Step1 implements Runnable {

		String sql;
		Connection conn;

		public Step1(Connection conn, String sql) {
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
					rateMatrix.put(rs.getLong(1), new HashMap<Long, Integer>());
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
			synchronized (counter) {
				counter.increaseCounter();
				counter.notify();
			}
			System.out.println("step1  " + Thread.currentThread() + "   over");
		}

	}

	class Step2 implements Runnable {

		String sql;
		int score;
		Connection conn;

		public Step2(Connection conn, String sql, int score) {
			this.sql = sql;
			this.score = score;
			this.conn = conn;
		}

		public Step2(Connection conn, String sql) {
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
						synchronized (rateMatrix) {
							Map<Long, Integer> map = null;
							// try{
							map = rateMatrix.get(rs.getLong(1));
							long member = rs.getLong(2);
							if (map != null && map.containsKey(member)) {
								map.put(member, map.get(member) + score);
							} else if (map != null) {
								map.put(member, score);
							}
						}

					}
				} else {
					while (rs.next()) {
						Map<Long, Integer> map = rateMatrix.get(rs.getLong(1));
						long member = rs.getLong(2);
						synchronized (rateMatrix) {
							if (map != null && map.containsKey(member)) {
								map.put(member, map.get(member) + rs.getInt(3));
							} else if (map != null) {
								map.put(member, rs.getInt(3));
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
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				synchronized (counter) {
					if (counter.getCounter() == 4) {
						SqlUtil.close();
						counter.notify();
					}
				}
			}
			System.out.println("step2  " + Thread.currentThread() + "   over");
		}
	}
}

class Counter {
	private int counter = -1;// 加不加volatile差不多

	public Counter(int counter) {
		this.counter = counter;
	}

	public Counter() {

	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	public void increaseCounter() {
		++counter;
	}

}
