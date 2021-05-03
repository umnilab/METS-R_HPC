package com.metsr.hpc;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MabManager{
	private Mab mab;
	private MabBus mabBus;
	private HashMap<Integer, ArrayList<Double>> initialLinkSpeedLength;
	private ConcurrentHashMap<Integer, Double> roadLengthMap; //July,2020
	public String data_dir;

	public MabManager(String data_dir_loc){
		HashMap<String, ArrayList<ArrayList<Integer>>> path_info = new HashMap<String, ArrayList<ArrayList<Integer>>>();
		HashMap<String, ArrayList<Integer>> valid_path = new HashMap<String, ArrayList<Integer>>();
		// July,2020
		HashMap<String, ArrayList<ArrayList<Integer>>> path_info_bus = new HashMap<String, ArrayList<ArrayList<Integer>>>();
		HashMap<String, ArrayList<Integer>> valid_path_bus = new HashMap<String, ArrayList<Integer>>();
		mab = new Mab(path_info, valid_path);
		//July, 2020
		mabBus = new MabBus(path_info_bus,valid_path_bus);
		initialLinkSpeedLength = new HashMap<Integer, ArrayList<Double>>();
		//July, 2020
		roadLengthMap = new ConcurrentHashMap<Integer,Double>();
		data_dir = data_dir_loc;
	}

	// it should be called every time tick.  [t1,t2]
	// input is the OD.  t1.
	// output is the route.  t1. 
	// refresh the energy.  t2.
	// t1, t1.
	
	public int ucbRouting(String od){
		mab.play(od);
		return mab.getAction();
	}
	
	// t2.
	public void refreshLinkUCB(ConcurrentHashMap<Integer, ArrayList<Double>> linkUCBMap){
//		synchronized (linkUCBMap) {
		mab.updateLinkUCB(linkUCBMap);
//		}
		
	}	
	
	public void refreshRouteUCB(ConcurrentHashMap<String, List<List<Integer>>> routeUCBMap){
//		synchronized (routeUCBMap) {
		mab.updateRouteUCB(routeUCBMap);
//		}
	}
	
	//July, 2020
	public int ucbRoutingBus(String od){
		mabBus.playBus(od);
		return mabBus.getAction();
	}
	
	//July, 2020 routeUCB bus
	public void refreshRouteUCBBus(ConcurrentHashMap<String, List<List<Integer>>> routeUCBMapBus){
//		synchronized (routeUCBMapBus) {
		mabBus.updateRouteUCBBus(routeUCBMapBus);
//		}
		
	}
	//July, 2020
	public void refreshLinkUCBBus(ConcurrentHashMap<Integer, ArrayList<Double>> linkUCBMapBus){
//		synchronized (linkUCBMapBus){
		mabBus.updateLinkUCBBus(linkUCBMapBus);
//		}
	}
	
	//July, 2020
	public void refreshLinkUCBShadow(ConcurrentHashMap<Integer, ArrayList<Double>> speedUCBMap, Map<Integer, Double> lengthUCB){
//		synchronized (speedUCBMap){
		mabBus.updateShadowBus(speedUCBMap,lengthUCB);
//		}
	}	
	
	public void initializeLinkEnergy1() {
		try {
			/* CSV file for data attribute */
			String fileName1 = data_dir +"/NYC/background_traffic/background_traffic_NYC.csv";
			BufferedReader br = new BufferedReader(new FileReader(fileName1));
			//br.readLine();          //the first row is the title row
			String line = null; 
			br.readLine();
			while ((line = br.readLine())!=null) {
				//line = br.readLine();
				String[] result1 = line.split(",");
				int roadID = Integer.parseInt(result1[0]);
				double SpeedSum = 0.0;
				for (int i = 1; i < 25; i++) {
					SpeedSum += Double.parseDouble(result1[i]);
				}
				double averageSpeed = SpeedSum/24.0;
				ArrayList<Double> speedLength = new ArrayList<Double>();
				speedLength.add(averageSpeed);
				initialLinkSpeedLength.put(roadID, speedLength);	
			}
			br.close();
		}catch (FileNotFoundException e){
			System.out.println("ContextCreator: No speed csv file found");
			e.printStackTrace();
		}catch (IOException e){
	        e.printStackTrace();
	    }
	}
	
	public void initializeLinkEnergy2(){
		try {
			/* CSV file for data attribute */
			String fileName2 = data_dir+"/NYC/background_traffic/background_traffic_NYC.csv";
			BufferedReader br = new BufferedReader(new FileReader(fileName2));
			br.readLine();          //the first row is the title row
			String line = null; 
			while ((line = br.readLine())!=null) {
				//line = br.readLine();
				String[] result=line.split(",");
				int roadID = Integer.parseInt(result[0]);
				double roadLength = Double.parseDouble(result[result.length-1]);
				ArrayList<Double> speedLength = new ArrayList<Double>();
				
				speedLength.add(initialLinkSpeedLength.get(roadID).get(0));
				speedLength.add(roadLength);
				initialLinkSpeedLength.put(roadID, speedLength);
				roadLengthMap.put(roadID,roadLength);  //July,2020
			}
			br.close();
		}catch (FileNotFoundException e){
			System.out.println("ContextCreator: No road csv file found");
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
		mab.warm_up(initialLinkSpeedLength);
		
		//July, 2020
		mabBus.warm_up_bus(initialLinkSpeedLength);
		//July, 2020
		
		
	}
	
	// July,2020
	public ConcurrentHashMap<Integer, Double> getRoadLengthMap(){
	    	return roadLengthMap;
	}
}
