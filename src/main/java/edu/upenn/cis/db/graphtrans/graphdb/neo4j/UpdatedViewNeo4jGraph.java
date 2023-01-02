package edu.upenn.cis.db.graphtrans.graphdb.neo4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.neo4j.cypher.result.QueryResult.Record;
import org.neo4j.graphdb.Result;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.Neo4j.Neo4jServerThread;
import edu.upenn.cis.db.datalog.DatalogClause;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.graphtrans.GraphTransServer;
import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
import edu.upenn.cis.db.graphtrans.datastructure.TransRuleList;
import edu.upenn.cis.db.graphtrans.parser.QueryToCypherParser;
import edu.upenn.cis.db.graphtrans.store.Store;
import edu.upenn.cis.db.graphtrans.store.StoreResultSet;
import edu.upenn.cis.db.graphtrans.store.neo4j.Neo4jStore;
import edu.upenn.cis.db.helper.Util;

public class UpdatedViewNeo4jGraph implements Neo4jGraph {
	/**
	 * Create a view by updating the graph instance. 
	 * This is the output graph of transformations but not a view.
	 * 
	 * @param transRuleList
	 */
	private static ArrayList<String> rules;
	private static StringBuilder rule;

	private static ArrayList<String> nodeLabels;
	private static ArrayList<String> edgeTypes;

	private static boolean useCopy = false;
	private static long level = 0;

	private Neo4jServerThread neo4jServer;
	private boolean useJavaCopy = false;
	
	private boolean hasMerge = false;
	private boolean hasIterBody = false;

	public UpdatedViewNeo4jGraph(Neo4jServerThread neo4jServer) {
		this.neo4jServer = neo4jServer;
	}

	public void createView(Store store, TransRuleList transRuleList) {
		System.out.println("[createViewByUpdate]");

		rules = new ArrayList<String>();
		String viewType = transRuleList.getViewType();

		int tid = Util.startTimer();

		String viewName = transRuleList.getViewName();
		String baseName = transRuleList.getBaseName();

		//		System.out.println("viewName: " + viewName + " baseName: " + baseName);

		useCopy = Config.isUseCopyForUpdatedViewNeo4jGraph();
		level = transRuleList.getLevel();
		setCypherQueryForUpdate(store, transRuleList);

		System.out.println("useCopy: " + useCopy);
		System.out.println("[createViewByUpdate] all rules: " + rules + "\n\n");

//		String testQuery = "MATCH (n1:MA)-[e1:L]->(n2:AA) WHERE n1.uid < 300000 RETURN count(*)";
//		Result resultXX = ((Neo4jStore)store).execute(testQuery);
//		System.out.println("resultXX: " + resultXX.resultAsString());
//		
		
//		testQuery = "// Transformation\n"
//				+ "CALL apoc.periodic.iterate('\n"
//				+ "MATCH (n1:MA)-[e1:L]->(n2:AA)\n"
//				+ "WHERE n1.uid < 300000\n"
//				+ "RETURN [n1, n2] as _s, e1\n LIMIT 1"
//				+ "','\n"
//				+ "CALL apoc.refactor.mergeNodes(_s, {properties: \"combine\"}) yield node // merge nodes\n"
//				+ "WITH node as _s\n"
//				+ "CALL apoc.create.setLabels(_s, [\"S\"]) YIELD node RETURN count(*)\n"
//				+ "', {batchSize:1, parallel:false})";
		
//		testQuery = "// Transformation\n"
//		+ "MATCH (n1:MA)-[e1:L]->(n2:AA)\n"
//		+ "WHERE n1.uid < 300000\n"
//		+ "WITH [n1, n2] as _s, e1\n"
//		+ "CALL apoc.refactor.mergeNodes(_s, {properties: \"combine\"}) yield node // merge nodes\n"
//		+ "WITH node as _s\n"
//		+ "CALL apoc.create.setLabels(_s, [\"S\"]) YIELD node RETURN count(*)\n";
////		+ "', {batchSize:1, parallel:false})";
//
//		resultXX = ((Neo4jStore)store).execute(testQuery);
//		System.out.println("resultXX2S: " + resultXX.resultAsString());
//		
//		System.exit(0);

		int tid2 = Util.startTimer();
//
//		String ss;
//		Result result1;
//		
//		ss = "CALL apoc.periodic.iterate("
//				+ "'MATCH (w:W)-[e1:WS]->(s:S), (s:S)-[e2:SL]->(l:L) "
//				+ "RETURN w, e1, s, e2, l', "  
//				+ "'CREATE (w)-[c:C]->(l)'"
////				+ "'CALL apoc.create.relationship(w,\"C\",{c:1,d:99},l) yield rel as r1 "
////				+ "CALL apoc.create.relationship(w,\"WW\",{c:1,d:99},l) yield rel as r2 "
////				+ "RETURN count(*)'"
//				+ ", {batchSize:10000, parallel:false})";
//		result1 = ((Neo4jStore)store).execute(ss);
//
//		System.out.println("324234 time: " + Util.getElapsedTime(tid2));
//		System.out.println("324234 result1: " + result1);


//		ss = "CALL apoc.periodic.iterate("
//				+ "'MATCH (s1:S)-[e1:SL]->(l1:L), (s2:S)-[e2:SL]->(l2:L), (l1:L)-[e3:A]->(l2:L) "
//				+ "RETURN s1, e1, l1, s2, e2, l2, e3',"
//				+ "'MERGE (s1)-[c:AS]->(s2) SET c.level=1'"
//				+ ", {batchSize:10000, parallel:false})";
//		result1 = ((Neo4jStore)store).execute(ss);
//
//		System.out.println("324234--- time: " + Util.getElapsedTime(tid2));
//		System.out.println("324234--- result1: " + result1);
		
//		
		for (int i = 0; i < rules.size(); i++) {
			//			System.out.println("i[" + i + "]\n" + rules.get(i).toString());
			System.out.println("i[" + i + "] out of " + rules.size() + " rule: " + rules.get(i)); //rules.get(i).substring(0, 25));

			Result result = ((Neo4jStore)store).execute(rules.get(i).toString());
			System.out.println("i[" + i + "] time (after commit): " + Util.getElapsedTime(tid2));
//			System.out.println("result: " + result.resultAsString());
			//			store.printResult(result);
		}
		
//		if (edgeTypes != null) {
//			for (String n : edgeTypes) {
//				String query = "// COUNT transformed\nmatch ()-[e]->() return type(e), e.level, count(*)";
//				((Neo4jStore)store).execute(query);
//			}
//		}
		
//		//		System.out.println("======all node===");
//		//		result = store.execute("MATCH (n) RETURN n, labels(n)");
//		//		store.printResult(result);
//		//		System.out.println("fsdfsdf1233");
//		//		result = store.execute("MATCH (n)-[r]->(m) RETURN *");
//		//		System.out.println("HHH");
//		//		store.printResult(result);
	}

//	public void createViewByOverlay(TransRuleList transRuleList) {
//		System.out.println("[createViewByOverlay]");
//		throw new UnsupportedOperationException();
//	}

	private void setCypherQueryForUpdate(Store store, TransRuleList transRuleList) {
		if (useCopy == true) {
			setNodeLabelsAndEdgeTypes(store);
			addCopyWholeGraph();
		}

//		try {
//			Thread.sleep(5*60*1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		for (TransRule tr : transRuleList.getTransRuleList()) {
			rule = new StringBuilder("// Transformation\n");
			
			hasIterBody = false;
			if (tr.getMapMap().size() > 0) {
				hasMerge = true;
			}
			
			rule.append("CALL apoc.periodic.iterate('\n");
			HashSet<String> vars = addMatchClause(tr.getPatternMatch(), tr.getWhereConditionForNeo4j());	
			addWithClause(tr, vars);
			addMergeNodeClause(tr.getMapMap());
			rule.append("','");
			addAddClause(tr.getPatternAdd());
			addRemoveClause(tr.getPatternRemove());
			if (hasIterBody == false ) {
				rule.append("RETURN count(*)\n");
			}
			rule.append("'\n");
			rule.append(", {batchSize:10000, parallel:false})\n");
//			addReturnClause();

//			System.out.println("rule.toString(): " + rule.toString());
			rules.add(rule.toString());
		}		
	}
	
	private void setNodeLabelsAndEdgeTypes(Store store) {
		// TODO Auto-generated method stub
		nodeLabels = new ArrayList<String>();
		edgeTypes = new ArrayList<String>();
		
		String query = "// Get Node Labels\n"
				+ "CALL db.labels() yield label\n"
				+ "return *\n";
		Result result = ((Neo4jStore)store).execute(query);
//		System.out.println("result1123: " + result.resultAsString());
		while (result.hasNext()) {
	        Map<String, Object> col = result.next();
	        Object label = col.get("label");
	        nodeLabels.add(label.toString());
		}
		
		query = "// Get Relationship Types\n"
				+ "CALL db.relationshipTypes() yield relationshipType\n"
				+ "return *\n";
		result = ((Neo4jStore)store).execute(query);
//		System.out.println("result1141423: " + result.resultAsString());
		while (result.hasNext()) {
	        Map<String, Object> col = result.next();
	        Object type = col.get("relationshipType");
	        edgeTypes.add(type.toString());
		}
		
		System.out.println("nodeLabels: " + nodeLabels);
		System.out.println("edgeTypes: " + edgeTypes);
		
//		System.exit(0);
	}

	private void addAddClause(ArrayList<Atom> atoms) {
		addRemoveNodeEdgeClause(atoms, true);
	}
	
	private void addRemoveClause(ArrayList<Atom> atoms) {
		addRemoveNodeEdgeClause(atoms, false);
	}

	private void addRemoveNodeEdgeClause(ArrayList<Atom> atoms, boolean isAdd) {
		// TODO Auto-generated method stub
//		System.out.println("[addRemoveNodeEdgeClause] atoms: " + atoms + " isAdd: " + isAdd);
//		rule = new StringBuilder();
//		if (isAdd == true) {
//			rule = new StringBuilder("// Add new node/edge\n");
//		} else {
//			rule = new StringBuilder("// Remove new node/edge\n");
//		}
		
		
		for (Atom a : atoms) {
			if (isAdd == false) {
				throw new NotImplementedException("Remove node/edge");
			}

			if (a.getRelName().equals(Config.relname_node) == true) {
				throw new NotImplementedException("Add new node");
			} else if (a.getRelName().equals(Config.relname_edge) == true) {
				String from = a.getTerms().get(1).getVar();
				String to = a.getTerms().get(2).getVar();
				String var = a.getTerms().get(1).getVar();
				String label = a.getTerms().get(3).toString();//.getSimpleTerm().getString();

				rule.append("CREATE (")
				.append(from).append(")-[xx:").append(Util.removeQuotes(label)).append(" {level:1}]->(")
				.append(to).append(")\n");

//				rule.append("CALL apoc.create.relationship(")
//					.append(from).append(",").append(label)
//					.append(",").append("{level:1},").append(to).append(") ")
//					.append("yield rel RETURN count(*)")
//					.append("\n");
			}
			hasIterBody = true;
		}
		
//		if (rule.length() > 0) {
//		System.out.println("rule1112: " + rule.toString());
//			rules.add(rule.toString());
//		}
	}

	private void addCopyWholeGraph() {
		// TODO Auto-generated method stub
		
		//rule = new StringBuilder("// Add indexes on nid/eid to copied graph\n")
		//		.append("CREATE INDEX index_node_nid FOR (n:L) ON (n.nid)\n");
		//rules.add(rule.toString());

		if (useJavaCopy == false) {
//			rule = new StringBuilder("// Initialize set node property\n")
//					.append("CALL apoc.periodic.iterate(\n")
//					.append("\"MATCH (a) RETURN a\"\n")
//					.append(", \"SET a.level = 0, a.id = id(a)\"\n")
//					.append(", {batchSize:5000, parallel:true});");
//			rules.add(rule.toString());
//
//			rule = new StringBuilder("// Initialize set edge property\n")
//					.append("CALL apoc.periodic.iterate(\n")
//					.append("\"MATCH ()-[r]->() RETURN r\"\n")
//					.append(", \"SET r.level = 0, r.id = id(r)\"\n")
//					.append(", {batchSize:5000, parallel:true});");
//			rules.add(rule.toString());

//			rule = new StringBuilder("// Add indexes to input graph\n")
//					.append("CALL db.labels() yield label\n")
//					.append("WITH label, ['nid'] as indexProp\n")
//					.append("WITH collect(label) as labels, collect(indexProp) as indexProps\n")
//					.append("WITH apoc.map.fromLists(labels, indexProps) as indexMap\n")
//					.append("call apoc.schema.assert(indexMap,{}) yield label, key, unique, action\n")
//					.append("return count(*);\n");
//			rules.add(rule.toString());

//			rule = new StringBuilder("// Level=0 for default\n")
//					.append("MATCH (n) WHERE n.level is null\n")
//					.append("SET n.level = 0\n")
//					.append("RETURN count(*)\n");
//			rules.add(rule.toString());
//
//			rule = new StringBuilder("// Level=0 for default\n")
//					.append("MATCH (a)-[e]->(b) WHERE e.level is null\n")
//					.append("SET e.level = 0\n")
//					.append("RETURN count(*)\n");
//			rules.add(rule.toString());
				
			for (String n : nodeLabels) {
				
				rule = new StringBuilder("// Clone nodes (prep count)\n")
						.append("MATCH (n:").append(n).append(") RETURN count(*)\n");
				rules.add(rule.toString());
				
				rule = new StringBuilder("// Clone nodes\n")
						.append("CALL apoc.periodic.iterate('MATCH (n:").append(n).append(") WHERE n.level = 0 RETURN n.uid as n_uid'\n")
						.append(", 'CREATE (nn:").append(n).append(" {uid: n_uid, level: 1})'\n")
//						.append(", 'CALL apoc.create.node(labels(n), {nid: n.nid, level: 1}) YIELD node RETURN count(*)'\n")
						.append(", {batchSize:5000, parallel:false})");
	//					.append("MATCH (n)\n")
	//					.append("CALL apoc.create.node(labels(n), {nid: n.nid, level: 1}) YIELD node\n")
	//					.append("RETURN count(*)\n");
				rules.add(rule.toString());
			}
			

			rule = new StringBuilder("// Cloned nodes\n")
//					.append("MATCH (n) WHERE  n.level = 0 RETURN n, labels(n), id(n) LIMIT 5");
					.append("MATCH (n) RETURN n.level, count(*)");
			rules.add(rule.toString());
			
			rule = new StringBuilder("// Cloned nodes duplicated\n")
					.append("MATCH (n) WITH n.uid as uid, n.level as level, count(*) as cnt WHERE cnt > 1 RETURN count(*)");
			rules.add(rule.toString());
			
//
//			rule = new StringBuilder("// Cloned nodes\n")
//					.append("MATCH (n) WHERE n.level = 1 RETURN n, labels(n), id(n) LIMIT 5");
//			rules.add(rule.toString());
			
			rule = new StringBuilder("// Add indexes to copied graph\n")
					.append("CALL db.labels() yield label\n")
					.append("WITH label, ['uid'] as indexProp\n")
					.append("WITH collect(label) as labels, collect(indexProp) as indexProps\n")
					.append("WITH apoc.map.fromLists(labels, indexProps) as indexMap\n")
					.append("call apoc.schema.assert(indexMap,{}) yield label, key, unique, action\n")
					.append("return *;\n");
			rules.add(rule.toString());
			
			
			
			rule = new StringBuilder("// Add indexes to copied graph\n")
			.append("CALL db.labels() yield label\n")
			.append("WITH label, ['uid'] as indexProp\n")
			.append("WITH collect(label) as labels, collect(indexProp) as indexProps\n")
			.append("WITH apoc.map.fromLists(labels, indexProps) as indexMap\n")
			.append("call apoc.schema.assert(indexMap,{}) yield label, key, unique, action\n")
			.append("return *;\n");
			rules.add(rule.toString());
	
//			String qqq = "// Clone relationships (TEST)\n"
//				+ "MATCH (n1)-[r:P]->(n2), (n3), (n4) "
//				+ "WHERE labels(n1) = labels(n3) AND n1.uid = n3.uid "
//				+ "AND labels(n2) = labels(n4) AND n2.uid = n4.uid "
//				+ "AND n1.level = 0 AND n2.level = 0 "
//				+ "AND n3.level = 1 AND n4.level = 1 "
//				+ "RETURN count(*)";
//			rules.add(qqq);

			for (String n : edgeTypes) {
				rule = new StringBuilder("// Clone relationships\n")
						.append("CALL apoc.periodic.iterate(\n")
						.append("\"MATCH (n1)-[r:").append(n).append("]->(n2), (n3), (n4)\n")
						.append("WHERE labels(n1) = labels(n3) AND n1.uid = n3.uid\n")
						.append("AND labels(n2) = labels(n4) AND n2.uid = n4.uid\n")
						//.append("WHERE n1.id = n3.source AND n2.id = n4.source\n")
						.append("AND n1.level = 0 AND n2.level = 0\n")
						.append("AND n3.level = 1 AND n4.level = 1\n")
						.append("RETURN n3, r, n4\"\n")
						.append(", \"CREATE (n3)-[xx:").append(n).append(" {uid:r.uid, level:1}]->(n4)\"\n")				
//						.append(", \"CALL apoc.create.relationship(n3, type(r), {uid:r.uid, level:1}, n4) ")
//						.append("YIELD rel RETURN count(*)\"\n")
						.append(", {batchSize:50000, parallel:false});");
				
//						.append("CALL apoc.periodic.iterate('MATCH (n:").append(n).append(") RETURN n.nid as n_nid LIMIT 1000000'\n")
//						.append(", 'CREATE (n:").append(n).append(" {nid: n_nid, level: 1})'\n")
////						.append(", 'CALL apoc.create.node(labels(n), {nid: n.nid, level: 1}) YIELD node RETURN count(*)'\n")
//						.append(", {batchSize:5000, parallel:true})");
//	//					.append("MATCH (n)\n")
//	//					.append("CALL apoc.create.node(labels(n), {nid: n.nid, level: 1}) YIELD node\n")
//	//					.append("RETURN count(*)\n");
				rules.add(rule.toString());
				
				rule = new StringBuilder("// Cloned edges\n")
//						.append("MATCH (n) WHERE  n.level = 0 RETURN n, labels(n), id(n) LIMIT 5");
						.append("MATCH (n1)-[e]->(n2) RETURN n1.level, e.level, n2.level, count(*)");
				rules.add(rule.toString());
	//				
			}
			

			rule = new StringBuilder("// Cloned edges duplicated\n")
					.append("MATCH ()-[e]->() WITH e.uid as uid, e.level as level, count(*) as cnt WHERE cnt > 1 RETURN count(*)");
			rules.add(rule.toString());
			
//			rule = new StringBuilder("// Clone relationships\n")
//					.append("CALL apoc.periodic.iterate(\n")
//					.append("\"MATCH (n1)-[r]->(n2), (n3), (n4)\n")
//					.append("WHERE labels(n1) = labels(n3) AND n1.nid = n3.nid\n")
//					.append("AND labels(n2) = labels(n4) AND n2.nid = n4.nid\n")
//					//.append("WHERE n1.id = n3.source AND n2.id = n4.source\n")
//					.append("AND n1.level = 0 AND n2.level = 0\n")
//					.append("AND n3.level = 1 AND n4.level = 1\n")
//					.append("RETURN n3, r, n4\"\n")
//					.append(", \"CREATE (n3)-[:F {eid:r.eid, level:1}]->(n4)\"\n")				
////					.append(", \"CALL apoc.create.relationship(n3, type(r), {eid:r.eid, level:1}, n4) ")
////					.append("YIELD rel RETURN count(*)\"\n")
//					.append(", {batchSize:50000, parallel:false});");
//			rules.add(rule.toString());

			rule = new StringBuilder("// Cloned relations\n")
					.append("MATCH (v1)-[n]->(v2) WHERE n.level = 1 RETURN v1,n,v2 LIMIT 10");
			rules.add(rule.toString());
		
		
		} else {
			int tid = Util.startTimer();
			neo4jServer.cloneAllNodes();
			System.out.println("Elapsed time for cloneAllNodes: " + Util.getElapsedTime(tid));
			neo4jServer.cloneAllEdges();
			System.out.println("Elapsed time for cloneAllEdges: " + Util.getElapsedTime(tid));
		}
	}

	private void addReturnClause() {
		rule.append("RETURN COUNT(*);");
	}

	private void addWithClause(TransRule transrule, HashSet<String> vars) {
		HashMap<Atom, HashSet<String>> maps = transrule.getMapMap();

		if (hasMerge == true) {
			rule.append("WITH ");
		} else {
			rule.append("RETURN ");
		}
		if (maps.size() > 0) {
			int i = 0;
			for (Atom a : maps.keySet()) {
				HashSet<String> sources = maps.get(a);
				vars.removeAll(sources);

				if (i > 0) {
					rule.append(", ");
				}
				rule.append(sources)
				.append(" as ")
				.append("_" + a.getTerms().get(0).toString());
				i++;
			}
			rule.append(", ");
		}
		int i = 0;
		for (String v : vars) {
			if (i > 0) {
				rule.append(", ");
			}
			rule.append(v);
			i++;
		}
		rule.append("\n");
	}

	private void addMergeNodeClause(HashMap<Atom, HashSet<String>> hashMap) {
		//		System.out.println("[addMergeNodeClause]");
		//		System.out.println("hashMap: " + hashMap);

		for (Atom a : hashMap.keySet()) {
			hasMerge = true;
			
//			System.out.println("a234234: " + a);
			HashSet<String> sources = hashMap.get(a);

//			+ "MATCH (n1:MA)-[e1:L]->(n2:AA)\n"
//			+ "WHERE n1.uid < 300000\n"
//			+ "WITH [n1, n2] as _s, e1\n"
//			+ "CALL apoc.refactor.mergeNodes(_s, {properties: \"combine\"}) yield node // merge nodes\n"
//			+ "WITH node as _s\n"
//			+ "CALL apoc.create.setLabels(_s, [\"S\"]) YIELD node RETURN count(*)\n";

			rule.append("CALL apoc.refactor.mergeNodes(")
			.append("_" + a.getTerms().get(0).toString())
			.append(", {properties: \"combine\"}) yield node") // as ")
			//.append(a.getTerms().get(0).toString())
			.append(" // merge nodes\n");
			
			String label = Util.removeQuotes(a.getTerms().get(1).toString());
			rule.append("WITH ")
			.append("node as _" + a.getTerms().get(0).toString() + "\n")
			.append("CALL apoc.create.setLabels(")
			.append("_" + a.getTerms().get(0).toString())
			.append(", [\"").append(label).append("\"]) ")
			.append("YIELD node RETURN *\n");
//			.append("// set labels\n");
			
			
//			rule.append("CALL apoc.refactor.mergeNodes(")
//			.append("_" + a.getTerms().get(0).toString())
//			.append(", {properties: \"combine\"}) yield node") // as ")
//			//.append(a.getTerms().get(0).toString())
//			.append(" // merge nodes\n");
//			
//			rule.append("WITH ")
//			.append("node as _" + a.getTerms().get(0).toString() + "\n")
//			.append("CALL apoc.create.setLabels(")
//			.append("_" + a.getTerms().get(0).toString())
//			.append(", [\"S\"]) ")
//			.append("YIELD node RETURN count(*)\n")
//			.append("// set labels\n");
		}

	}

	private HashSet<String> addMatchClause(ArrayList<Atom> atoms, ArrayList<String> whereConditionsForNeo4j) {
		ArrayList<ArrayList<String>> edges = new ArrayList<ArrayList<String>>();
		HashMap<String, String> nodes = new HashMap<String, String>(); // var, label

		for (int i = 0; i < atoms.size(); i++) {
			Atom a = atoms.get(i);
			//			System.out.println("a: " + a);
			if (a.getPredicate().getRelName().contentEquals("E") == true) { // edge
				ArrayList<String> edge = new ArrayList<String>();
				edge.add(a.getTerms().get(0).toString()); // edgeVar
				edge.add(a.getTerms().get(1).toString()); // edgeFrom
				edge.add(a.getTerms().get(2).toString()); // edgeTo
				edge.add(Util.removeQuotes(a.getTerms().get(3).toString())); // edgeLabel
				edges.add(edge);
			} else if (a.getRelName().contentEquals(Config.relname_node) == true) { // node
				String nodeVar = a.getTerms().get(0).toString();
				String nodeLabel = Util.removeQuotes(a.getTerms().get(1).toString());
				nodes.put(nodeVar, nodeLabel);
			}
		}

		System.out.println("edges: " + edges);
		System.out.println("nodes: " + nodes);
		
		HashSet<String> vars = new LinkedHashSet<String>();
		rule.append("MATCH ");
		for (int i = 0; i < edges.size(); i++) {
			ArrayList<String> edge = edges.get(i);

			if (i > 0) {
				rule.append(", ");
			}
			rule.append("(")
			.append(edge.get(1))
			.append(":")
			.append(nodes.get(edge.get(1)))
			.append(")-[")
			.append(edge.get(0))
			.append(":")
			.append(edge.get(3))
			.append("]->(")
			.append(edge.get(2))
			.append(":")
			.append(nodes.get(edge.get(2)))
			.append(")");

			vars.add(edge.get(1));
			vars.add(edge.get(0));
			vars.add(edge.get(2));
		}
		rule.append("\n");

		int countForLevel = 0;
		if (whereConditionsForNeo4j.size() > 0) {
			for (String cond : whereConditionsForNeo4j) { 
				if (countForLevel == 0) {
					rule.append("WHERE ");
				} else {
					rule.append(" AND ");
				}
				rule.append(cond);
				countForLevel++;
			}
		}
		if (useCopy == true) {
			for (Map.Entry<String, String> entry : nodes.entrySet()) {
			    String key = entry.getKey();
			    String value = entry.getValue();

			    if (countForLevel == 0) {
					rule.append("WHERE ");
				} else {
					rule.append(" AND ");
				}
				rule.append(key)
				.append(".level = ")
				.append(level);
				countForLevel++;
			}
				
			for (int i = 0; i < edges.size(); i++) {
				ArrayList<String> edge = edges.get(i);

			    if (countForLevel == 0) {
					rule.append("WHERE ");
				} else {
					rule.append(" AND ");
				}

			    rule.append(edge.get(0))
				.append(".level = ")
				.append(level);
				countForLevel++;
			}
		}
		if (countForLevel > 0) {
			rule.append("\n");
		}

		System.out.println(Util.ANSI_RED + "whereConditionsForNeo4j: " + whereConditionsForNeo4j + Util.ANSI_RESET);

		return vars;
	}

	@Override
	public String getCypher(String query) {
		// TODO Auto-generated method stub
//		StoreResultSet rs = new StoreResultSet();
		
		System.out.println("[getCypher] query: " + query);
		
		QueryToCypherParser parser = new QueryToCypherParser();
		String cypher = null;
		
		if (useCopy == true) {
			cypher = parser.getCypherWithLevel(query);
		} else {
			cypher = parser.getCypherWithoutLevel(query);
		}
		System.out.println("[getCypher] cypher: " + cypher);
		return cypher;
	}
	
	public static void main(String[] args) throws Exception {
		Config.initialize();
		System.out.println("[UpdatedViewNeo4jGraph]");
		Config.setUseCopyForUpdatedViewNeo4jGraph(true);
		
		Neo4jGraph neo4jGraph = new UpdatedViewNeo4jGraph(null);
		
		String query = "match s:S-e:X->t:T, r:R, t-e1:Y->v:V from v0 where s<= 100 return s-e->t";
				
		String cypher = neo4jGraph.getCypher(query);
		
		System.out.println("cypher: " + cypher);
	}
}
