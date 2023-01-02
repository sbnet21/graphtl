package edu.upenn.cis.db.graphtrans.graphdb.neo4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.neo4j.graphdb.Result;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.Neo4j.Neo4jServerThread;
import edu.upenn.cis.db.datalog.DatalogClause;
import edu.upenn.cis.db.datalog.DatalogProgram;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.graphtrans.GraphTransServer;
import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
import edu.upenn.cis.db.graphtrans.datastructure.TransRuleList;
import edu.upenn.cis.db.graphtrans.parser.QueryToCypherParser;
import edu.upenn.cis.db.graphtrans.store.Store;
import edu.upenn.cis.db.graphtrans.store.StoreResultSet;
import edu.upenn.cis.db.graphtrans.store.neo4j.Neo4jStore;
import edu.upenn.cis.db.helper.Util;

public class OverlayViewNeo4jGraph implements Neo4jGraph {
	/**
	 * Create a view by updating the graph instance. 
	 * This is the output graph of transformations but not a view.
	 * 
	 * @param transRuleList
	 */
	private static ArrayList<String> rules;
	private static StringBuilder rule;
	private int newid = 100000000;
		
	public OverlayViewNeo4jGraph() {
//		newid = 100000000; 
	}
	public void createView(Store store, TransRuleList transRuleList) {
		System.out.println("[OverlayViewNeo4jGraph]");

		rules = new ArrayList<String>();
		String viewType = transRuleList.getViewType();
		
		int tid = Util.startTimer();
		
		String viewName = transRuleList.getViewName();
		String baseName = transRuleList.getBaseName();
		
//		System.out.println("viewName: " + viewName + " baseName: " + baseName);
		setCypherQueryForUpdate(transRuleList);
		
//		System.out.println("[OverlayViewNeo4j] [createView] rules: " + rules);

		for (int i = 0; i < rules.size(); i++) {
			System.out.println("rule i: " + i + " => " + rules.get(i).toString());
			Result result = ((Neo4jStore)store).execute(rules.get(i).toString());
//			store.printResult(result);
		}

		
		//		System.out.println("======all node===");
//		result = store.execute("MATCH (n) RETURN n, labels(n)");
//		store.printResult(result);
//		System.out.println("fsdfsdf1233");
//		result = store.execute("MATCH (n)-[r]->(m) RETURN *");
//		System.out.println("HHH");
//		store.printResult(result);
	}
	
	private void setCypherQueryForUpdate(TransRuleList transRuleList) {
		addBaseRule();
		
		for (TransRule tr : transRuleList.getTransRuleList()) {
			rule = new StringBuilder("// Transformation\n");
			rule.append("CALL apoc.periodic.iterate('\n");
			HashSet<String> vars = addMatchClause(tr.getPatternMatch(), tr.getWhereConditionForNeo4j(), null);	
			addWithClause(tr, vars);
			rule.append("','\n");
			addMergeNodeClause(tr.getMapMap());
			addAddClause(tr.getPatternAdd());
			addRemoveClause(tr.getPatternRemove());
			rule.append("'\n");
			rule.append(", {batchSize:10000, parallel:false})\n");
//			addReturnClause();

//			System.out.println("rule.toString(): " + rule.toString());
			rules.add(rule.toString());
		}
		
//		rules.add("CALL apoc.help(\"apoc\");");

		DatalogProgram p = GraphTransServer.getProgram();
		for (int i = 0; i < transRuleList.getTransRuleList().size(); i++) {
			TransRule tr = transRuleList.getTransRuleList().get(i);

			String viewname = transRuleList.getViewName();
			String matchname = "MATCH_" + viewname + "_" + i;
			
			DatalogClause dc = p.getRules(matchname).get(0);
			HashSet<String> edgeVars = new HashSet<String>();
			for (Atom e : dc.getBody()) {
				if (e.getRelName().startsWith("E") == true) {
					edgeVars.add(e.getTerms().get(0).getVar());
				}
			}
			System.out.println("[OverlayView] edgeVars: " + edgeVars);
//			String mapname = "MAP_" + viewname;

			String procName = "n4ivm_" + i;
			rule = new StringBuilder("// Procedure\n");
			rule.append("CALL apoc.custom.asProcedure(\n")
				.append("  '").append(procName).append("',")
				.append("'\n")
				.append("// do nothing\n");
				
			HashSet<String> vars = addMatchClause(tr.getPatternMatch(), tr.getWhereConditionForNeo4j(), edgeVars);
			rule.append("WITH ");
			
			HashMap<Atom, HashSet<String>> maps = tr.getMapMap();
			if (maps.size() > 0) {
				int j = 0;
				for (Atom a : maps.keySet()) {
					HashSet<String> sources = maps.get(a);
					vars.removeAll(sources);

					if (j > 0) {
						rule.append(", ");
					}
					rule.append(sources)
						.append(" as ")
						.append("_" + a.getTerms().get(0).toString());
					j++;
					
					String repSource = null;
					for (String s : sources) {
						repSource = s;
						break;
					}
					rule.append(",")
						.append(repSource);
				}
//				rule.append(", ");
			}		
			rule.append("\n");
			
//			addWithClause(tr, vars);
//			rule.append("','\n");
			addMergeNodeClause(tr.getMapMap());
				
//			rule.append("CREATE (n:W {uid:"+4832904*(i+1)+",c:0,d:99})\n")
//				.append("RETURN n.uid AS answer;\n")
			rule.append("',\n")
				.append("'write',\n")
				.append("[['answer', 'int']],\n") 
				.append("[['eid', 'int']]\n")  
				.append(");");
			System.out.println("[setCypherQueryForUpdate] rule: " + rule.toString());
			rules.add(rule.toString());
			
			Neo4jServerThread.getEdgeTriggers().add(procName);
		}
	}
	
	private void addAddClause(ArrayList<Atom> atoms) {
		addRemoveNodeEdgeClause(atoms, true);
	}
	
	private void addRemoveClause(ArrayList<Atom> atoms) {
		addRemoveNodeEdgeClause(atoms, false);
	}

	private void addRemoveNodeEdgeClause(ArrayList<Atom> atoms, boolean isAdd) {
		// TODO Auto-generated method stub
		System.out.println("[addRemoveNodeEdgeClause] atoms: " + atoms + " isAdd: " + isAdd);
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
				.append(from).append(")-[xx:").append(Util.removeQuotes(label)).append(" {c:1,d:99}]->(")
				.append(to).append(")\n");

				
//				rule.append("CALL apoc.create.relationship(")
//					.append(from).append(",").append(label)
//					.append(",").append("{c:1,d:99},").append(to).append(") ")
//					.append("yield rel")
//					.append("\n");
			}
		}
		
//		if (rule.length() > 0) {
//		System.out.println("rule1112: " + rule.toString());
//			rules.add(rule.toString());
//		}
	}
	
	
	private void addBaseRule() {
//		rule = new StringBuilder("// Level=0 for default\n")
//				.append("MATCH (n) WHERE n.c is null\n")
//				.append("SET n.c = 0, n.d = 99\n")
//				.append("RETURN count(*)\n");
//		rules.add(rule.toString());
//
//		rule = new StringBuilder("// Level=0 for default\n")
//				.append("MATCH (a)-[e]->(b) WHERE e.c is null\n")
//				.append("SET e.c = 0, e.d = 99\n")
//				.append("RETURN count(*)\n");
//		rules.add(rule.toString());
	}

	private void addReturnClause() {
		rule.append("RETURN COUNT(*);");
	}
	
	private void addWithClause(TransRule transrule, HashSet<String> vars) {
		HashMap<Atom, HashSet<String>> maps = transrule.getMapMap();

		rule.append("RETURN ");
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
				
				String repSource = null;
				for (String s : sources) {
					repSource = s;
					break;
				}
				rule.append(",")
					.append(repSource);
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
		
		if (hashMap.size() == 0) {
			return;
		}
		
		for (Atom a : hashMap.keySet()) {
			HashSet<String> sources = hashMap.get(a);
			String repSource = null;
			for (String s : sources) {
				repSource = s;
				break;
			}
			
			String target = a.getTerms().get(0).toString();
			String source = "_" + a.getTerms().get(0).toString();
			String label = Util.removeQuotes(a.getTerms().get(1).toString());
			
			rule.append("CREATE (")
				.append(target)
				.append(":")
				.append(label)
				.append(" {uid:" + repSource + ".uid+10000000, c:1, d:99})")
				.append(" // merge nodes\n");
			newid++;
		}
		
		rule.append("WITH ");
		int i = 0; 
		for (Atom a : hashMap.keySet()) {
			String target = a.getTerms().get(0).toString();
			String source = "_" + a.getTerms().get(0).toString();
			
			if (i > 0) {
				rule.append(",");
			}
			rule.append(source)
				.append(", ")
				.append(target);
			i++;
		}
		rule.append("\nCALL {\n");
		i = 0;
		for (Atom a : hashMap.keySet()) {
			String target = a.getTerms().get(0).toString();
			String source = "_" + a.getTerms().get(0).toString();
			if (i > 0) {
				rule.append("UNION\n");
			}
			// incoming to source nodes
			rule.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nUNWIND ")
				.append(source)
				.append(" AS ess\n")
				.append("SET ess.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append("\nMATCH (a)-[r]->(ess)\n")
				.append("WHERE NOT(a IN ")
				.append(source)
				.append(")\n")
				.append("SET r.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append(", a, r\n")
				.append("CALL apoc.create.relationship(a, type(r), {uid: r.uid, c:1,d:99}, ")
				.append(target)
				.append(") YIELD rel\n")
				.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nRETURN 1\n");
			// outgoing from source nodes
			rule.append("UNION\n")	
				.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nUNWIND ")
				.append(source)
				.append(" AS ess\n")
				.append("SET ess.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append("\nMATCH (ess)-[r]->(a)\n")
				.append("WHERE NOT(a IN ")
				.append(source)
				.append(")\n")
				.append("SET r.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append(", a, r\n")
				.append("CALL apoc.create.relationship(")
				.append(target)
				.append(", type(r), {uid: r.uid, c:1,d:99}, ")
				.append("a")
				.append(") YIELD rel\n")
				.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nRETURN 1\n");
			// internal edges between source nodes
			rule.append("UNION\n")
				.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nUNWIND ")
				.append(source)
				.append(" AS ess\n")
				.append("SET ess.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append("\nMATCH (a)-[r]->(ess)\n")
				.append("WHERE a IN ")
				.append(source)
				.append("\n")
				.append("SET r.d = 1\n")
				.append("WITH ")
				.append(source)
				.append(", ess, ")
				.append(target)
				.append(", a, r\n")
				.append("CALL apoc.create.relationship(")
				.append(target)
				.append(", type(r), {uid: r.uid, c:1,d:99}, ")
				.append(target)
				.append(") YIELD rel\n")
				.append("WITH ")
				.append(source)
				.append(", ")
				.append(target)
				.append("\nRETURN 1\n");
			i++;
		}
		rule.append("}\n")
			.append("RETURN count(*)\n");
	}
	
	private HashSet<String> addMatchClause(ArrayList<Atom> atoms, ArrayList<String> whereConditionsForNeo4j, HashSet<String> edgeVars) {
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
		if (edgeVars != null) {
			for (String e : edgeVars) {
				if (countForLevel == 0) {
					rule.append("WHERE (");
				} else {
					rule.append("OR ");
					
				}
				rule.append(e)
					.append(".uid=")
					.append("$eid ");
				countForLevel++;
			}
			rule.append(")");
		}
		
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
		if (countForLevel > 0) {
			rule.append("\n");
		}

		
		System.out.println(Util.ANSI_RED + "whereConditionsForNeo4j: " + whereConditionsForNeo4j + Util.ANSI_RESET);
		
//		for (int i = 0; i < atoms.size(); i++) {
//			Atom a = atoms.get(i);
////			System.out.println("a: " + a);
//			if (a.getPredicate().getRelName().contentEquals("E") == true) { // edge
//				ArrayList<String> edge = new ArrayList<String>();
//				edge.add(a.getTerms().get(0).toString()); // edgeVar
//				edge.add(a.getTerms().get(1).toString()); // edgeFrom
//				edge.add(a.getTerms().get(2).toString()); // edgeTo
//				edge.add(Util.removeQuotes(a.getTerms().get(3).toString())); // edgeLabel
//				edges.add(edge);
//			} else { // node
//				String nodeVar = a.getTerms().get(0).toString();
//				String nodeLabel = Util.removeQuotes(a.getTerms().get(1).toString());
//				nodes.put(nodeVar, nodeLabel);
//			}
//		}
//		
//		HashSet<String> vars = new LinkedHashSet<String>();
//		rule.append("MATCH ");
//		for (int i = 0; i < edges.size(); i++) {
//			ArrayList<String> edge = edges.get(i);
//			
//			if (i > 0) {
//				rule.append(", ");
//			}
//			rule.append("(")
//				.append(edge.get(1))
//				.append(":")
//				.append(nodes.get(edge.get(1)))
//				.append(")-[")
//				.append(edge.get(0))
//				.append(":")
//				.append(edge.get(3))
//				.append("]->(")
//				.append(edge.get(2))
//				.append(":")
//				.append(nodes.get(edge.get(2)))
//				.append(")");
//			
//			vars.add(edge.get(1));
//			vars.add(edge.get(0));
//			vars.add(edge.get(2));
//		}
//		rule.append("\n");
		
		return vars;
	}

	@Override
	public String getCypher(String query) {
		// TODO Auto-generated method stub
//		StoreResultSet rs = new StoreResultSet();
		
		System.out.println("[getCypher] query: " + query);
		
		QueryToCypherParser parser = new QueryToCypherParser();
		String cypher = null;
		
		cypher = parser.getCypherWithCreatedDestroyed(query);

		System.out.println("[getCypher] cypehr: " + cypher);
		return cypher;
	}
	

}
