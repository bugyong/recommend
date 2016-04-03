package recommend;

import java.util.Map;

import recommend.RecommendMatrix.GoodAndRate;

public class Test {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		/*
		 * PeopleMatrix pm = new PeopleMatrix(); Map<Long, Map<Long, Integer>>
		 * createPeopleMatrix = pm.createPeopleMatrix();
		 * System.out.println("end");
		 */
		RecommendMatrix recommendMatrix = new RecommendMatrix();
		Map<Long, Map<GoodAndRate, Integer>> createRecommendMatrix = recommendMatrix.createRecommendMatrix();
		System.out.println("main end");
	}

}
