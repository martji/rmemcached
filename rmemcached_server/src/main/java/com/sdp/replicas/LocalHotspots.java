package com.sdp.replicas;

import java.util.HashSet;
import java.util.Set;

public class LocalHotspots {
	private static Set<String> hotspots = new HashSet<String>();
	private static final String table = "usertable:user";
	
	public static boolean contains(String key) {
		return hotspots.contains(key);
	}
	
	public static void add(Integer keynum) {
		hotspots.add(table + Utils.hash(keynum));
	}
}
