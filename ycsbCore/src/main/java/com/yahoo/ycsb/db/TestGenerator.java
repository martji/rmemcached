package com.yahoo.ycsb.db;

import com.yahoo.ycsb.generator.HotspotIntegerGenerator;

public class TestGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HotspotIntegerGenerator generator = new HotspotIntegerGenerator(0,100,0.2,0.8);
		int[] counters = new int[101];
		int opcount = 10000;
		for (int i = 0; i < opcount; i++) {
			int index = generator.nextInt();
			counters[index] ++;
		}
		for (int i = 0; i < 101; i++) {
			System.out.println(i + "\t" + counters[i]);
		}
	}

}
