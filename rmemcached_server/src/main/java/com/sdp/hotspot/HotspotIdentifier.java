package com.sdp.hotspot;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import com.sdp.monitor.Monitor;

public class HotspotIdentifier {
	Long currenTimestamp;
	int sliceId;
	Queue<String> tmpLowQueue = new LinkedList<String>();
	Map<String, Integer> tmpHighMap = new HashMap<String, Integer>();
	Map<String, RankItem> hotspotMap = new HashMap<String, RankItem>();
	
	final int tmpLowQueueSize = 1000;
	final double cputhreod = 80;
	
	public HotspotIdentifier (Long timestamp) {
		this.currenTimestamp = timestamp;
		sliceId = 0;
		getNextSlice();
	}
	
	public void handleRegister(Long timestamp, String key) {
		if (timestamp < currenTimestamp) {
			if (tmpHighMap.containsKey(key)) {
				tmpHighMap.put(key, tmpHighMap.get(key) + 1);
			} else if (tmpLowQueue.contains(key)) {
				tmpHighMap.put(key, 2);
			} else {
				tmpLowQueue.offer(key);
				if (tmpHighMap.size() > tmpLowQueueSize) {
					tmpLowQueue.poll();
				}
			}
		} else {
			Map<String, Integer> highMap = new HashMap<String, Integer>();
			for (Entry<String, Integer> e : tmpHighMap.entrySet()) {
				highMap.put(e.getKey(), e.getValue());
			}
			caculaterHotspot(sliceId, highMap);
			
			tmpLowQueue = new LinkedList<String>();
			tmpHighMap = new HashMap<String, Integer>();
			getNextSlice();
		}
	}
	
	private void caculaterHotspot(final int sliceId, final Map<String, Integer> highMap) {
		Runnable runnable = new Runnable() {
			public void run() {
				for (Entry<String, Integer> e : highMap.entrySet()) {
					String key = e.getKey();
					int count = e.getValue();
					CountItem countItem = new CountItem(sliceId, count);
					if (hotspotMap.containsKey(key)) {
						hotspotMap.get(key).addCount(countItem);
					} else {
						RankItem rankItem = new RankItem(key);
						rankItem.addCount(countItem);
						hotspotMap.put(key, rankItem);
					}
				}
				
				if (Monitor.getInstance().cpuCost > cputhreod) {
					String hotKey = null;
					Double weight = 0.0;
					for (Entry<String, Integer> e : highMap.entrySet()) {
						String key = e.getKey();
						RankItem rankItem = hotspotMap.get(key);
						if (rankItem.getWeight(sliceId) > weight) {
							weight = rankItem.getWeight(sliceId);
							hotKey = key;
						}
					}
					if (hotKey != null) {
						// ?
					}
				}
			}
		};
		new Thread(runnable).start();
	}

	public void getNextSlice() {
		currenTimestamp += 5*1000;
		sliceId += 1;
	}
	
}
