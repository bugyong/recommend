package recommend;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class RecommendMatrix {
	private static Map<Long, Map<GoodAndRate, Integer>> recommendMatrix = new TreeMap<Long, Map<GoodAndRate, Integer>>();
	Counter counter = new Counter(0);
	int threadNum = 1;

	public Map<Long, Map<GoodAndRate, Integer>> createRecommendMatrix() throws Exception {
		Map<Long, Map<Long, Integer>> peopleMatrix = new PeopleMatrix().createPeopleMatrix();
		Map<Long, Map<Long, Double>> simMatrix = new SimMatrix().createSimMatrix();
		Step7 step7 = new Step7(peopleMatrix, simMatrix);
		for (int i = 0; i < threadNum; i++) {
			new Thread(step7).start();
		}
		while (true) {
			synchronized (counter) {
				if (counter.getCounter() < threadNum) {
					counter.wait();
				} else {
					break;
				}
			}

		}
		return recommendMatrix;
	}

	class Step7 implements Runnable {
		Iterator<Entry<Long, Map<Long, Integer>>> peopleEntryiterator;
		Map<Long, Map<Long, Double>> simMatrix;

		public Step7(Map<Long, Map<Long, Integer>> peopleMatrix, Map<Long, Map<Long, Double>> simMatrix) {
			this.simMatrix = simMatrix;
			this.peopleEntryiterator = peopleMatrix.entrySet().iterator();
		}

		@Override
		public void run() {
			Map<Long, Double[]> map = new HashMap<Long, Double[]>();
			Entry<Long, Map<Long, Integer>> peopleEntry;
			while (true) {

				synchronized (peopleEntryiterator) {
					if (peopleEntryiterator.hasNext()) {
						peopleEntry = peopleEntryiterator.next();// 人，物，分
					} else {
						break;
					}
				}
				Map<Long, Integer> peopleMap = peopleEntry.getValue();// 物，分
				Set<Long> peopleRatedGoods = peopleMap.keySet();// 物
				Set<Entry<Long, Integer>> personEntry = peopleMap.entrySet();// 物，分
				for (Entry<Long, Integer> pe : personEntry) {
					accumulate(pe, map, peopleRatedGoods);
				}
				total(map, peopleEntry.getKey());// nullpointer
			}
			synchronized (counter) {
				counter.increaseCounter();
				if (counter.getCounter() == threadNum) {
					counter.notify();
				}
			}
		}

		private void total(Map<Long, Double[]> map, Long key) {

			Set<Entry<Long, Double[]>> entrySet = map.entrySet();
			TreeMap<GoodAndRate, Integer> treeMap = new TreeMap<GoodAndRate, Integer>();
			synchronized (recommendMatrix) {// nullpointer
				recommendMatrix.put(key, treeMap);
			}
			for (Entry<Long, Double[]> e : entrySet) {// 这里需要同步recommendMatrix吗

				GoodAndRate goodAndRate = new GoodAndRate();
				goodAndRate.setGood(e.getKey());
				goodAndRate.setRate(e.getValue()[0] / e.getValue()[1]);
				treeMap.put(goodAndRate, 0);
			}
		}

		private void accumulate(Entry<Long, Integer> pe, Map<Long, Double[]> map, Set<Long> peopleRatedGoods) {
			Long goodId = pe.getKey();
			Integer goodRate = pe.getValue();
			// System.out.println(goodId);
			Map<Long, Double> goodMap = simMatrix.get(goodId);
			Set<Entry<Long, Double>> entrySet = null;
			if (goodMap != null) {
				entrySet = goodMap.entrySet();// nullpointer
				Double[] record;
				for (Entry<Long, Double> e : entrySet) {
					// if()
					goodId = e.getKey();
					if (!peopleRatedGoods.contains(goodId)) {
						Long key = e.getKey();
						Double value = e.getValue();
						if (!map.containsKey(key)) {
							record = new Double[2];
							record[0] = value * goodRate;// 相似度*评分
							record[1] = value;
							map.put(key, record);
						} else {
							Double[] doubles = map.get(key);
							doubles[0] += value * goodRate;
							doubles[1] += value;
							map.put(key, doubles);
						}
					}
				}
			}

		}

	}

	class GoodAndRate implements Comparable<GoodAndRate> {
		Long good;
		Double rate;

		public Long getGood() {
			return good;
		}

		public void setGood(Long good) {
			this.good = good;
		}

		public Double getRate() {
			return rate;
		}

		public void setRate(Double rate) {
			this.rate = rate;
		}

		@Override
		public int compareTo(GoodAndRate g) {
			if (g.rate > rate)
				return -1;
			else if (g.rate < rate)
				return 1;
			else {
				if (g.good > good)
					return -1;
				else if (g.good < good)
					return 1;
			}
			return 0;
		}

		@Override
		public String toString() {
			return good + ":" + rate;
		}

	}
}
