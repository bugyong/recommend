package recommend;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SimMatrix {

	private Map<Long, Map<Long, Double>> simMatrix;
	private int rateMatrixSize;
	private Counter counter = new Counter();

	// NumberFormat nf;
	public Map<Long, Map<Long, Double>> createSimMatrix() throws Exception {
		// Counter counter = new Counter();
		int n = 4;// 线程数,4在本机就处于满负荷了
		// 1:27368
		// 2:7551
		// 3:6486
		// 4:5943
		simMatrix = new HashMap<Long, Map<Long, Double>>(10000);
		Map<Long, Map<Long, Integer>> rateMatrix = new RateMatrix().createRateMatrix();
		System.out.println("rate end");

		// nf = NumberFormat.getNumberInstance();
		// nf.setMaximumFractionDigits(2);

		rateMatrixSize = rateMatrix.size();

		Thread step3 = new Thread(new Step3(rateMatrix));
		step3.start();
		synchronized (counter) {
			while (counter.getCounter() < 0)
				counter.wait();
		}

		Step4 step4 = new Step4(rateMatrix, rateMatrixSize);
		for (int i = 0; i < n; i++) {
			new Thread(step4).start();
		}

		synchronized (counter) {
			while (counter.getCounter() != n)
				counter.wait();
		}
		return simMatrix;
	}

	public Map<Long, Map<Long, Double>> getSimMatrix() {
		return simMatrix;
	}

	/*
	 * public void setSimMatrix(Map<Long, Map<Long, Double>> simMatrix) {
	 * this.simMatrix = simMatrix; }
	 */

	class Step3 implements Runnable {

		Map<Long, Map<Long, Integer>> rateMatrix;

		public Step3(Map<Long, Map<Long, Integer>> rateMatrix) {
			this.rateMatrix = rateMatrix;

		}

		@Override
		public void run() {
			Set<Long> keySet = rateMatrix.keySet();
			for (Long key : keySet) {
				simMatrix.put(key, new HashMap<Long, Double>(10000));
			}
			synchronized (counter) {
				counter.increaseCounter();
				counter.notify();
			}
		}
	}

	class Step4 implements Runnable {

		Map<Long, Map<Long, Integer>> rateMatrix;
		AtomicInteger ai = new AtomicInteger(0);
		int totalRow;
		Iterator<Entry<Long, Map<Long, Integer>>> goods1EntryIter;

		public Step4(Map<Long, Map<Long, Integer>> rateMatrix, int totalRow) {
			this.rateMatrix = rateMatrix;
			this.totalRow = totalRow;
			goods1EntryIter = rateMatrix.entrySet().iterator();
		}

		@Override
		public void run() {

			int row = 1;
			while (true) {
				Entry<Long, Map<Long, Integer>> good1Entry = null;
				synchronized (goods1EntryIter) {
					if ((row = ai.incrementAndGet()) < totalRow) {
						good1Entry = goods1EntryIter.next();
					} else {
						break;
					}
				}
				down(row, good1Entry);
			}

			System.out.println(Thread.currentThread() + " over");
			synchronized (counter) {
				counter.increaseCounter();
				counter.notify();
			}
		}// run

		private void down(int row, Entry<Long, Map<Long, Integer>> good1Entry) {
			Long good1 = good1Entry.getKey();
			Iterator<Entry<Long, Map<Long, Integer>>> goods2EntryIter = rateMatrix.entrySet().iterator();
			for (int i = 0; i < row; i++) {
				goods2EntryIter.next();
			}
			Map<Long, Integer> people1AndRateMap = good1Entry.getValue();

			while (goods2EntryIter.hasNext()) {
				Iterator<Long> people1 = people1AndRateMap.keySet().iterator();// 每次重新获得
				Entry<Long, Map<Long, Integer>> goods2Entry = goods2EntryIter.next();
				Long good2 = goods2Entry.getKey();
				Map<Long, Integer> people2AndRateMap = goods2Entry.getValue();
				Set<Long> people2 = people2AndRateMap.keySet();

				double sim = 0;
				boolean hasSim = false;
				while (people1.hasNext()) {
					Long person1 = people1.next();
					if (people2.contains(person1)) {
						sim += Math.pow(people1AndRateMap.get(person1) - people2AndRateMap.get(person1), 2);
						hasSim = true;
					}
				}
				if (hasSim) {
					sim = Math.round((1 / (1 + Math.sqrt(sim)) * 100)) / 100.0;// nf.format很耗时
				}
				simMatrix.get(good1).put(good2, sim);
				simMatrix.get(good2).put(good1, sim);
			}
		}// down
	}// step4

}
