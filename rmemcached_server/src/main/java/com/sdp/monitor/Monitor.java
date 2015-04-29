package com.sdp.monitor;
import java.io.BufferedReader;  
import java.io.InputStreamReader;  

import org.jboss.netty.channel.Channel;
  
public class Monitor {  
	static Monitor monitor = null;
	int memcachedPort;
	Channel monitorChannel;
	public double cpuCost = 0.0;
	
	public static Monitor getInstance() {
		if (monitor == null) {
			monitor = new Monitor();
		}
		return monitor;
	}
	
    public double getCpuCost(int port){  
    	double cpuCost = 0.0;
        try {  
            String shcmd = System.getProperty("user.dir") + "/scripts/monitor.sh " + port;  
            Process ps = Runtime.getRuntime().exec(shcmd);  
            ps.waitFor(); 

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));  
            String result;  
            result = br.readLine();  
            if (result == null) {
                System.out.println("no result!"); 
            }
            String[] paras = result.split("\\s+");
            System.out.println("Port " + port + " cpuCost: " + paras[8]);
            cpuCost = Double.parseDouble(paras[8]);
            
            br.close();
            monitor.cpuCost = cpuCost;
        }   
        catch (Exception e) {  
            e.printStackTrace();  
        }  
        return cpuCost;
    }

	public void setPort(int memcached) {
		monitor.memcachedPort = memcached;
	}
	
	public int getPort() {
		return monitor.memcachedPort;
	}

	public Double getCpuCost() {
		return getCpuCost(monitor.memcachedPort);
	}

	public void setMonitorChannel(Channel getmChannel) {
		monitor.monitorChannel = getmChannel;
	}
}