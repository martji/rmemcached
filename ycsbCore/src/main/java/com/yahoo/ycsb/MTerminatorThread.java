/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.
 */
package com.yahoo.ycsb;

import java.util.Vector;

/**
 * A thread that waits for the maximum specified time and then interrupts all
 * the client threads passed as the Vector at initialization of this thread.
 * 
 * The maximum execution time passed is assumed to be in seconds.
 * 
 * @author sudipto
 * 
 */
public class MTerminatorThread extends Thread {

	private Vector<Thread> threads;
	private Vector<Workload> workloadList;
	private long waitTimeOutInMS;

	public MTerminatorThread(Vector<Thread> threads, Vector<Workload> workloadList) {
		this.threads = threads;
		this.workloadList = workloadList;
		waitTimeOutInMS = 2000;
	}

	public void run() {
		for(int i = 0; i < workloadList.size(); i++) {
			Workload workload = workloadList.get(i);
			long maxExecutionTime = workload.maxExecutionTime;
			System.err.println("Stage " + i + ": Maximum execution time specified as: " + maxExecutionTime + " secs");
			try {
				Thread.sleep(maxExecutionTime * 1000);
			} catch (InterruptedException e) {
				System.err.println("Could not wait until max specified time, TerminatorThread interrupted.");
				return;
			}
			System.err.println("Stage " + i + ": Maximum time elapsed. Requesting stop for the workload.");
			workload.requestStop();
		}
		System.err.println("Stop requested for workload. Now Joining!");
		for (Thread t : threads) {
			while (t.isAlive()) {
				try {
					t.join(waitTimeOutInMS);
					if (t.isAlive()) {
						System.err.println("Still waiting for thread "
								+ t.getName() + " to complete. ");
					}
				} catch (InterruptedException e) {
				}
			}
		}
	}
}
