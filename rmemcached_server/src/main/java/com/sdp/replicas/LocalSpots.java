package com.sdp.replicas;

import java.util.HashSet;
import java.util.Set;

public class LocalSpots {
	public static Set<String> hotspots = new HashSet<String>();
	public static Set<String> coldspots = new HashSet<String>();
	private static final String table = "usertable:user";
	
	public static int threshold = 10000;
	public static int coldThreshold = 1;
	
	public static boolean containsHot(String key) {
		return hotspots.contains(key);
	}
	
	public static void addHot(Integer keynum) {
		hotspots.add(table + keynum);
	}

	public static void addHot(String key) {
		hotspots.add(key);
	}

	public static void removeHot(String key) {
		hotspots.remove(key);
	}
	
	public static boolean containsCold(String key) {
		return coldspots.contains(key);
	}
	
	public static void addCold(Integer keynum) {
		coldspots.add(table + Utils.hash(keynum));
	}

	public static void addCold(String key) {
		coldspots.add(key);
	}

	public static void removeCold(String key) {
		coldspots.remove(key);
	}
}
