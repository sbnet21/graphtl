//package edu.upenn.cis.db.graphtrans.graphdb.datalog;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedHashSet;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import edu.upenn.cis.db.ConjunctiveQuery.Atom;
//import edu.upenn.cis.db.ConjunctiveQuery.Predicate;
//import edu.upenn.cis.db.ConjunctiveQuery.Term;
//import edu.upenn.cis.db.datalog.DatalogClause;
//import edu.upenn.cis.db.datalog.DatalogProgram;
//import edu.upenn.cis.db.datalog.simpleengine.LongSimpleTerm;
//import edu.upenn.cis.db.datalog.simpleengine.SimpleTerm;
//import edu.upenn.cis.db.datalog.simpleengine.StringSimpleTerm;
//import edu.upenn.cis.db.graphtrans.Config;
//import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
//import edu.upenn.cis.db.graphtrans.datastructure.TransRuleList;
//import edu.upenn.cis.db.graphtrans.store.Store;
//import edu.upenn.cis.db.helper.Util;
//
///**
// * Generate Datalog rules from a view definition
// * 
// * @author sbnet21
// *
// */
//public class ViewRuleGenWithSingleId {
//	final static Logger logger = LogManager.getLogger(ViewRuleGenWithSingleId.class);
//	
//	private static int rid = 0;
//	private static String viewName;
//	private static String baseName;
//
//	private static String MAP;
//	private static String NDA;
//	private static String NDD;
//	private static String EDA;
//	private static String EDD;
//	private static String N0;
//	private static String E0;
//	private static String N1;
//	private static String E1;
//	private static HashSet<String> headVars;
//	private static DatalogProgram program;
//	
//	private static void initialize(String base, String view) {
//		baseName = base;
//		viewName = view;
//		headVars = new LinkedHashSet<String>();
//
//		MAP = Config.relname_mapping + "_" + viewName;
//		NDA = Config.relname_node + "_delta_" + Config.relname_added + "_" + viewName;
//		NDD = Config.relname_node + "_delta_" + Config.relname_deleted + "_" + viewName;
//		EDA = Config.relname_edge + "_delta_" + Config.relname_added + "_" + viewName;
//		EDD = Config.relname_edge + "_delta_" + Config.relname_deleted + "_" + viewName;
//		N0 = Config.relname_node + "_" + baseName;
//		E0 = Config.relname_edge + "_" + baseName;
//		N1 = Config.relname_node + "_" + viewName;
//		E1 = Config.relname_edge + "_" + viewName;
//	}
//	
//	private static void addDeltasFromMappigsToProgram() {
////		StringBuilder rule = new StringBuilder("# Delta rules\n");
//
//		Atom a = null;
//		DatalogClause c = null;
//
//		// add node
//		c = new DatalogClause();
//		a = new Atom(NDA, "id", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(MAP, "_u1", "_u2", "id", "label");
//		c.addAtomToBody(a);
//		program.addRule(c);
//		
//		// remove node
//		c = new DatalogClause();
//		a = new Atom(NDD, "m", "m_label");
//		c.addAtomToHeads(a);
//		a = new Atom(MAP, "m", "m_label", "_u1", "_u2");
//		c.addAtomToBody(a);
//		program.addRule(c);
//		
//		// deltas for edge
//		// add edge
//		c = new DatalogClause();
//		a = new Atom(EDA, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "_from", "to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "_from", "_u1", "from", "_u2");
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "to", "_u3", "_u4", "_u5");
//		a.setNegated(true);
//		c.addAtomToBody(a);
//		program.addRule(c);
//			
//		c = new DatalogClause();
//		a = new Atom(EDA, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "from", "_to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "from", "_u2", "_from", "_u3");
//		a.setNegated(true);
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "_to", "_u4", "to", "_u5");
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		c = new DatalogClause();
//		a = new Atom(EDA, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "_from", "_to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "_from", "_u1", "from", "_u2");
//		c.addAtomToBody(a);
//		a = new Atom(MAP, "_to", "_u3", "to", "_u4");
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		// remove edge
//		c = new DatalogClause();
//		a = new Atom(EDD, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "from", "to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(NDD, "from", "_u1");
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		c = new DatalogClause();
//		a = new Atom(EDD, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "from", "to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(NDD, "to", "_u1");
//		c.addAtomToBody(a);
//		program.addRule(c);
//	}
//		
//	/**
//	 * Construct view based on base graph and deltas
//	 * 
//	 * @param viewName
//	 * @param baseName
//	 * @return
//	 */	
//	public static void addViewFromBaseAndDeltasRule(String viewName, String baseName) {
////		StringBuilder rule = new StringBuilder();
//		Atom a = null;
//		DatalogClause c = null;
//
//		// Construct view from base and deltas
//		// View construction rules
//		c = new DatalogClause();
//		a = new Atom(N1, "id", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(N0, "id", "label");
//		c.addAtomToBody(a);
//		a = new Atom(NDD, "id", "_u1");
//		a.setNegated(true);
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		c = new DatalogClause();
//		a = new Atom(N1, "id", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(NDA, "id", "label");
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		c = new DatalogClause();
//		a = new Atom(E1, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(E0, "id", "from", "to", "label");
//		c.addAtomToBody(a);
//		a = new Atom(EDD, "id", "_u1", "_u2", "_u3");
//		a.setNegated(true);
//		c.addAtomToBody(a);
//		program.addRule(c);
//
//		c = new DatalogClause();
//		a = new Atom(E1, "id", "from", "to", "label");
//		c.addAtomToHeads(a);
//		a = new Atom(EDA, "id", "from", "to", "label");
//		c.addAtomToBody(a);
//		program.addRule(c);
//	}
//
//	private static void addViewFromBaseAndDeltasRule() {
//		addViewFromBaseAndDeltasRule(viewName, baseName);
//	}
//	
//	private static ArrayList<Atom> getAtomOfBase(Atom atom) {
//		ArrayList<Atom> atoms = atom.getAtomBodyStrWithInterpretedAtoms(baseName);
//		
//		
//		headVars.addAll(atoms.get(0).getVars());
////		System.out.println("atoms[0]: " + atoms.get(0));
//		if (atoms.size() > 1) {
//			headVars.add(atoms.get(1).getTerms().get(0).getVar());
////			System.out.println("atoms[1]: " + atoms.get(1));
//		}
//		return atoms;
//	}
//	
//	private static void addSingleTransRuleToProgram(TransRule transRule, int index) {
////		StringBuilder rule = new StringBuilder("# Single Trans Rule\n");
//		
//		/**
//		 * 1. Construct RHS (Use MATCH relation)
//		 * 2. Map
//		 * 3. Add/remove
//		 */
//		String body = "";
//		String var_rep = null; // representative var
//		HashMap<String, Integer> metaMap = new HashMap<String, Integer>();
//		HashSet<String> newNodes = new HashSet<String>();
//		HashSet<String> newEdges = new HashSet<String>();
//		HashSet<String> vars = new HashSet<String>();
//		
//		DatalogClause clause_const = new DatalogClause();
//		DatalogClause clause_use = new DatalogClause();
//		
//		headVars.clear();
//		
//		// create body
//		for (int i = 0; i < transRule.getPatternMatch().size(); i++) {
//			if (var_rep == null) {
//				if (transRule.getPatternMatch().get(i).getPredicate().equals(Config.predN) == true) {
//					String var_rep_candidate = transRule.getPatternMatch().get(i).getTerms().get(0).getVar();
//					if (transRule.getStarVarSet().contains(var_rep_candidate) == false) {
//						var_rep = var_rep_candidate;
//					}
//				}
//			}
////			if (body.contentEquals("") == false) {
////				body += ", ";
////			}
////			body += getAtomOfBase(transRule.getPatternMatch().get(i));
//			
//			clause_const.getBody().addAll(getAtomOfBase(transRule.getPatternMatch().get(i)));
////			body += transRule.getPatternMatch().get(i).toString();
//			vars.addAll(transRule.getPatternMatch().get(i).getVars());
//			
//			for (HashMap.Entry<Integer, String> entry : transRule.getPatternMatch().get(i).getInterpreted().entrySet()) {
//				headVars.add(entry.getValue());
////				System.out.println("add HeadVar: " + var + " entry: " + entry);
//			}
////			System.out.println("body: " + body);
////			System.out.println("headVars: " + headVars);
//
//
//		}
////		System.out.println("body: " + body);
//
//
////		System.out.println("var_rep: " + var_rep);
//		
//		// mapping rules
////		rule.append("# Mapping Rules\n");
//		
////		System.out.println("===> " + transRule.getMapMap().entrySet());
//		for (HashMap.Entry<Atom, HashSet<String>> entry : transRule.getMapMap().entrySet()) {
//			String dstVar = entry.getKey().getTerms().get(0).toString();
//			String dstLabel = entry.getKey().getTerms().get(1).toString();
//			newNodes.add(dstVar);
//			//int rid = Util.getCounter("rid");
//			rid++;
//			for (String srcVar : entry.getValue()) {
//				String srcVarLabel = null;
//				for (Atom a : transRule.getPatternMatch()) {
//					if (a.getTerms().get(0).getVar().contentEquals(srcVar) == true) {
//						if (a.getTerms().get(1).isConstant() == true) {
//							srcVarLabel = a.getInterpreted().get(1);
//						} else {
//							srcVarLabel = a.getTerms().get(0).toString() + "_label";
//						}
//						break;
//					}
//				}
//				if (srcVarLabel == null) {
//					throw new IllegalArgumentException("srcVarLabel is null. srcVar: " + srcVar + ", transRule.getMapMap(): " + transRule.getMapMap());
//				}
//				
//				Atom h = new Atom(Config.relname_mapping + "_" + viewName, srcVar, srcVarLabel, dstVar, "label");
//				clause_use.addAtomToHeads(h);
//				Atom b = new Atom(Config.predOpEq);
//				b.getTerms().add(new Term("label", true));
//				b.getTerms().add(new Term(dstLabel, false));
//				clause_use.addAtomToBody(b);
//				
////				String subRule = Config.relname_mapping + "_" + viewName 
////						+ "(" + srcVar + ", " + srcVarLabel + ", " // source (members)
////						+ var_rep + ", label" // target (super node)
////						+ ") <- " 
////						+ body + ", label = " + dstLabel + ".\n";
////				rule.append(subRule);
//				metaMap.put(dstVar, rid);
//			}
//		}
//
////		rule.append("# add node/edge\n");
//		Atom head = null;
//
//		for (int i = 0; i < transRule.getPatternAdd().size(); i++) {
//			Atom a = transRule.getPatternAdd().get(i);
//			rid++;
//			if (a.getPredicate().equals(Config.predN)) {
//				newNodes.add(a.getTerms().get(0).getVar());
//				DatalogClause c1 = new DatalogClause();
//				head = new Atom(NDA, a.getTerms().get(0).getVar());
//				head.appendTerm(new Term(Util.removeQuotes(a.getTerms().get(1).toString()), false));
//				
////				rule.append(NDA + "(" + var_rep + ", " + 
////						a.getTerms().get(1).toString() + ") <- " + body + ".\n");
//				metaMap.put(a.getTerms().get(0).toString(), rid);
//			} else if (a.getPredicate().equals(Config.predE)) {
//				newEdges.add(a.getTerms().get(0).getVar());
////				rule.append(EDA + "(" + var_rep + ", ");
//				head = new Atom(EDA, a.getTerms().get(0).getVar());
//				
//				String from = a.getTerms().get(1).toString();
//				String to = a.getTerms().get(2).toString();
//				String label = a.getTerms().get(3).toString();
//				
////				if (metaMap.containsKey(from) == true) { // meta
////					head.appendTerm(new Term(var_rep, true));
//////					rule.append(var_rep + ", ");
////				} else {
//					head.appendTerm(new Term(from, true));
////					rule.append(from +", ");
////				}
//				
////				if (metaMap.containsKey(to) == true) { // meta
////					head.appendTerm(new Term(var_rep, true));
//////					rule.append(var_rep + ", ");
////				} else {
//					head.appendTerm(new Term(to, true));
////					rule.append(to + ", ");
////				}
//				head.appendTerm(new Term(label, false));
////				rule.append(label + ") <- " + body + ".\n");
//			}
//			clause_use.addAtomToHeads(head);
//		}
//
////		rule.append("# remove node/edge\n");
//		for (int i = 0; i < transRule.getPatternRemove().size(); i++) {
//			Atom a = transRule.getPatternRemove().get(i);
//
//			if (a.getPredicate().equals(Config.predN)) {
//				// FIXME: we currently assume that we remove non-meta node only (level is also specified)
//				String var = a.getTerms().get(0).toString();
//				String label = Util.removeQuotes(a.getTerms().get(1).toString());
//				head = new Atom(NDD, var);
//				head.appendTerm(new Term(label, false));
////				System.out.println("head: " + head);
////				rule.append(NDD + "(" + var + ", " + label + ") <- " + body + 
////						".\n");
//			} else if (a.getPredicate().equals(Config.predE)) {
//				String var = a.getTerms().get(0).toString();
//				String from = a.getTerms().get(1).toString();
//				String to = a.getTerms().get(2).toString();
//				String label = a.getTerms().get(3).toString();
//				head = new Atom(EDD, var, from, to);
//				head.appendTerm(new Term(label, false));
////				rule.append(EDA + "(" + var + ", 0, 0, \"*\")" +
////						" <- " + body + ".\n"); 
//			}
//			clause_use.addAtomToHeads(head);
//		}
//		
//		ArrayList<Atom> heads = new ArrayList<Atom>();
//
//		head = new Atom(new Predicate("MATCH_" + viewName + "_" + index));
//		for (String var : headVars) {
//			head.getTerms().add(new Term(var, true));
//		}
//		if (Config.isPostgres() == true) {
//			for (String v : newNodes) {
//				head.getTerms().add(new Term(v, true, true));
//			}
//			for (String v : newEdges) {
//				head.getTerms().add(new Term(v, true, true));
//			}
//		}
//		clause_const.addAtomToHeads(head);
//		clause_use.getBody().add(head);
//		
////		const_nid_r1[t1,t2] = n -> int(t1), int(t2), nid(n). // for each created node
////		lang:constructor(`const_nid_r1).
////		nid_r1_id(c,d,id) <- nid_id(n : id), const_nid_r1[c,d] = n.
//		
//		// const_nid_r1(c,d, newnode), nid(newnode),
//		HashSet<String> newSet;
//		String type;
//		for (int i = 0; i < 2; i++) {
//			if (i == 0) {
//				type = "nid";
//				newSet = newNodes;
//			} else {
//				type = "eid";
//				newSet = newEdges;
//			}
//			
//			for (String var : newSet) {
//				String relConst = "const_" + type + "_" + viewName + "_" + i + "_" + var;
//				String relGetId = type + "_" + viewName + "_" + i + "_" + var + "_id";
//				Atom a = new Atom(new Predicate(relConst));
//				
//				for (String v : vars) {
//					a.getTerms().add(new Term(v, true));
//				}
//				a.getTerms().add(new Term(var, true));
//				if (Config.isPostgres() == false) {
//					clause_const.addAtomToHeads(a);
//					clause_const.addAtomToHeads(new Atom(type, var));
//				} else {
//					clause_const.addAtomToBody(a);
//				}
//				
//				if (Config.isPostgres() == false) {
//					Atom b = new Atom(new Predicate(relGetId));
//					b.setTerms(a.getTerms());
//					clause_use.addAtomToBody(b);
//				}
//				String ruleId = viewName + "_" + i + "_" + var;
//				if (i == 0) {
//					program.getNodeConstructors().put(ruleId, vars.size());
//				} else {
//					program.getEdgeConstructors().put(ruleId, vars.size());
//				}
//	//			const_nid_r1[t1,t2] = n -> int(t1), int(t2), nid(n). // for each created node
//	//			lang:constructor(`const_nid_r1).
//	//			nid_r1_id(c,d,id) <- nid_id(n : id), const_nid_r1[c,d] = n.
//			}
//		}
//
//		for (int i = 0; i < heads.size(); i++) {
//			clause_use.addAtomToHeads(heads.get(i));
////			if (i > 0) {
////				rule.append(", ");
////			}
////			rule.append(heads.get(i));
//		}
////		rule.append(" <- ")
////			.append(body).append(".\n");
//		
//		program.addEDB(head.getPredicate().getRelName());
//		program.addRule(clause_const);
//		program.addRule(clause_use);
//		
//
////		System.out.println("clause_const: " + clause_const);
////		System.out.println("clause_use: " + clause_use);
////		System.out.println("headVars: " + headVars);
////		System.out.println("newNodes: " + newNodes);
////		System.out.println("newEdges: " + newEdges);
////
////		System.out.println("program32: " + program);
////		System.exit(0);
////		return rule.toString();
//	}
//
//	public static void addRuleToProgram(DatalogProgram p, TransRuleList transRuleList, boolean isAllIncluded) {
//		String viewType = transRuleList.getViewType();
////		System.out.println("viewType: " + viewType);
//		StringBuilder rule = new StringBuilder("# View Rule\n");
//		Util.resetCounter("rid");
//		rid = 0;
//		program = p;
//		initialize(transRuleList.getBaseName(), transRuleList.getViewName());
//		
//		if (isAllIncluded == true || viewType.contentEquals("virtual") == false) {
//			for (int i = 0; i < transRuleList.getNumTransRuleList(); i++) {
//				addSingleTransRuleToProgram(transRuleList.getTransRule(i), i);
//			}
//			addDeltasFromMappigsToProgram();
//		}
//		if (isAllIncluded == true || viewType.contentEquals("materialized") == true) {
//			addViewFromBaseAndDeltasRule();
//		}
//		
//		
////		System.out.println("program43: " + program.getString());
////		System.out.println("LB RULES: \n");
////		System.out.println(rule.toString());
////		System.out.println("=========\n");
//		
////		return rule.toString();
//	}
//	
//	public static void insertCatalogView(Store store, String name, String base, String type, String query, long level) {
//		ArrayList<SimpleTerm> args = new ArrayList<SimpleTerm>();
//		args.add(new StringSimpleTerm(name));
//		args.add(new StringSimpleTerm(base));
//		args.add(new StringSimpleTerm(type));
//		args.add(new StringSimpleTerm(Util.addSlashes(query)));
//		args.add(new LongSimpleTerm(level));
//		store.addTuple(Config.relname_catalog_view, args);
//	}
//}