package edu.upenn.cis.db.logicblox;

import org.junit.Before;
import org.junit.Test;

import com.logicblox.connect.ConnectBlox.Response;

import edu.upenn.cis.db.helper.Util;

public class LogicBloxTest {

	@Before
	public void setUp() throws Exception {
	}
	
	private String getSchema() {
		String str = "R(a,b) -> int(a), int(b).\n";
		str += "S(c,d) -> int(c), int(d).\n";
//		str += "R_(a,b) -> int(a), int(b).\n";
//		str += "S_(c,d) -> int(c), int(d).\n";
	
		str += "lang:derivationType[`A]=\"Derived\".\n";
		//str += "lang:derivationType[`A]=\"OnDemand\".\n";

		return str; 
	}
	
	private String getIDBRules() {
		String str = "A(a,b) <- R(a,c), S(c,b).\n";
//		str += "+S(a,b) <- S_(a,b).\n";
//		str += "+R(a,b) <- R_(a,b).\n";
		//rule += "+" + Config.relname_node_schema + postfix + "[label] = id <- uid<<id>> +" + Config.relname_node_schema_add + "(label).\n";

		return str;
	}
	
	private String getEDBfacts() {
		String str = "R(1,2). R(2,3). S(1,3), S(3,5).\n";
		
		return str;
	}
	
	@Test
	public void test() {
//		String workspace = "test";
//		String blockname = "";
//		
//		String logic = "";
//		
//		logic += getSchema();
//		logic += getIDBRules();
//		logic += getEDBfacts();
//		
////		LogicBlox.connect("127.0.0.1", 5518);
//		Util.resetTimer();
//		Response res = LogicBlox.runAddBlock("t", blockname, "");
//		System.out.println("ElapsedTime: " + Util.getLapTime());
//
//		res = LogicBlox.runAddBlock(workspace, blockname, logic);
////		
//		System.out.println(res);
//		
		
		//fail("Not yet implemented");
	}

}
