package edu.upenn.cis.db.graphtrans.experiment;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ExpConfigLoader {
    private static ArrayList<String> getStringArray(JSONArray ja) {
    	ArrayList<String> values = new ArrayList<String>();
        for (Iterator itr = ja.iterator(); itr.hasNext();) {
        	values.add((String)itr.next());
        }
    	return values;
    }

    private static ArrayList<Boolean> getBooleanArray(JSONArray ja) {
    	ArrayList<Boolean> values = new ArrayList<Boolean>();
        for (Iterator itr = ja.iterator(); itr.hasNext();) {
        	values.add((Boolean)itr.next());
        }
    	return values;
    }
    
    private static ArrayList<Long> getLongArray(JSONArray ja) {
    	ArrayList<Long> values = new ArrayList<Long>();
        for (Iterator itr = ja.iterator(); itr.hasNext();) {
        	values.add((Long)itr.next());
        }
    	return values;
    }   
    
    public static void load(String filename) {
    	// parsing file "JSONExample.json" 
        Object obj = null;
		try {
			obj = new JSONParser().parse(new FileReader(filename));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
          
        // typecasting obj to JSONObject 
        JSONObject jo = (JSONObject) obj; 
        
        ExpConfig.setConf("configFilePath", (String)jo.get("configFilePath"));
        ExpConfig.setConf("useRealExecution", (Boolean)jo.get("useRealExecution"));
        ExpConfig.setConf("printRules", (Boolean)jo.get("printRules"));
        ExpConfig.setConf("printTiming", (Boolean)jo.get("printTiming"));
        ExpConfig.setConf("printConsole", (Boolean)jo.get("printConsole"));
        
        ExpConfig.setConf("queryIteration", (Long)jo.get("queryIteration"));
        ExpConfig.setConf("expIteration", (Long)jo.get("expIteration"));
        ExpConfig.setConf("ivm_iteration", (Long)jo.get("ivm_iteration"));
        
        ExpConfig.setConf("doTypeCheck", (Boolean)jo.get("doTypeCheck"));
        ExpConfig.setConf("doVaryingSelectivity", (Boolean)jo.get("doVaryingSelectivity"));
        ExpConfig.setConf("doVaryingScale", (Boolean)jo.get("doVaryingScale"));
        ExpConfig.setConf("doVariousDataset", (Boolean)jo.get("doVariousDataset"));
        ExpConfig.setConf("doParallel", (Boolean)jo.get("doParallel"));
        ExpConfig.setConf("doParallelVarying", (Boolean)jo.get("doParallelVarying"));
        ExpConfig.setConf("doIVM", (Boolean)jo.get("doIVM"));
        
        ExpConfig.setConf("prune_typecheck", (Boolean)jo.get("prune_typecheck"));
        ExpConfig.setConf("prune_subquery", (Boolean)jo.get("prune_subquery"));
        
        ExpConfig.setConf("exp_indexes", getBooleanArray((JSONArray)jo.get("exp_indexes")));
        ExpConfig.setConf("exp_useQueries", getBooleanArray((JSONArray)jo.get("exp_useQueries")));
        ExpConfig.setConf("exp_viewtypes", getStringArray((JSONArray)jo.get("exp_viewtypes")));
        ExpConfig.setConf("exp_platforms", getStringArray((JSONArray)jo.get("exp_platforms")));
        ExpConfig.setConf("exp_sequential", getBooleanArray((JSONArray)jo.get("exp_sequential")));
        ExpConfig.setConf("update_ratio", getLongArray((JSONArray)jo.get("update_ratio")));
        ExpConfig.setConf("update_querySet", getLongArray((JSONArray)jo.get("update_querySet")));
         
    	ArrayList<String> datasets = getStringArray((JSONArray)((JSONObject)jo.get("datasets")).get("names"));
    	ArrayList<Boolean> datasets_execute = getBooleanArray((JSONArray)((JSONObject)jo.get("datasets")).get("execute"));
    	
    	ExpConfig.setConf("datasets",  datasets);
    	ExpConfig.setConf("datasets_execute",  datasets_execute);
    	
    	for (int i = 0; i < datasets.size(); i++) {
    		ExpDataset eds = new ExpDataset();
    		eds.setName(datasets.get(i));
    		eds.setExecute(datasets_execute.get(i));
    	
    		String key = "dataset_" + datasets.get(i);
    		JSONObject jo1 = (JSONObject)jo.get(key);
    		eds.setSynthetic((boolean)jo1.get("synthetic"));
    		
    		// CONSTRAINT
        	eds.setEgds(getStringArray((JSONArray)jo1.get("constraints")));
        	
        	// SCHEMAS
    		JSONObject jo2 = (JSONObject)jo1.get("schemas");
    		ArrayList<String> nodes = getStringArray((JSONArray)jo2.get("nodes"));
        	for (String node : nodes) {
        		eds.getSchemas().add("create node " + node);
        	}
        	JSONArray ja1 = (JSONArray)jo2.get("edges");
        	for (Iterator itr = ja1.iterator(); itr.hasNext();) {
        		ArrayList<String> edge = getStringArray((JSONArray)itr.next());
        		eds.getSchemas().add("create edge " + edge.get(0) + " (" + edge.get(1) + " -> " + edge.get(2) + ")");
	        }        	
        	
        	JSONArray ja2 = (JSONArray)jo1.get("rules");
        	for (Iterator itr = ja2.iterator(); itr.hasNext();) {
        		JSONArray ja3 = (JSONArray)itr.next();
        		ArrayList<ArrayList<String>> arr = new ArrayList<ArrayList<String>>();
        		for (Iterator itr2 = ja3.iterator(); itr2.hasNext();) {
        			arr.add(getStringArray((JSONArray)itr2.next()));
        		}
        		eds.getTransrules().add(arr);
        	}

        	JSONArray ja3 = (JSONArray)jo1.get("queries");
        	for (Iterator itr = ja3.iterator(); itr.hasNext();) {
        		eds.getQueries().add(getStringArray((JSONArray)itr.next()));        		
        	}
        	
        	if (eds.isSynthetic() == true) {
	        	// EXP_SIZE
	    		JSONObject jo3 = (JSONObject)jo1.get("exp_size");
	    		eds.setBase_selectivity((Long)jo3.get("selectivity"));
	    		eds.setSizes(getLongArray((JSONArray)jo3.get("data")));
	        	     	
	        	// EXP_SELECTIVITY
	    		JSONObject jo4 = (JSONObject)jo1.get("exp_selectivity");
	    		eds.setBase_size((Long)jo4.get("size"));
	    		eds.setSelectivities(getLongArray((JSONArray)jo4.get("data")));
        	}
        	
        	JSONObject jo3 = (JSONObject)jo1.get("csv");
        	if (jo3 != null) {
	    		eds.setCsv_default((String)jo3.get("default"));
	    		eds.setCsv_neo4j((String)jo3.get("neo4j"));
        	}
    		
    		ExpConfig.getDataSets().put(datasets.get(i), eds);
    	}
//        System.out.println(ExpConfig.getString());
    }
}
