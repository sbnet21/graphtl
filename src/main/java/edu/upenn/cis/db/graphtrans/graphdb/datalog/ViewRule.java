package edu.upenn.cis.db.graphtrans.graphdb.datalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.ConjunctiveQuery.Predicate;
import edu.upenn.cis.db.ConjunctiveQuery.Term;
import edu.upenn.cis.db.datalog.DatalogClause;
import edu.upenn.cis.db.datalog.DatalogProgram;
import edu.upenn.cis.db.datalog.simpleengine.LongSimpleTerm;
import edu.upenn.cis.db.datalog.simpleengine.SimpleTerm;
import edu.upenn.cis.db.datalog.simpleengine.StringSimpleTerm;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
import edu.upenn.cis.db.graphtrans.datastructure.TransRuleList;
import edu.upenn.cis.db.graphtrans.store.Store;
import edu.upenn.cis.db.helper.Util;

/**
 * Generate Datalog rules from a view definition
 * 
 * @author sbnet21
 *
 */
public class ViewRule {
	final static Logger logger = LogManager.getLogger(ViewRule.class);

	private static int rid = 0;
	private static String viewName;
	private static String baseName;

	private static String MAP;
	private static String NDA;
	private static String NDD;
	private static String EDA;
	private static String EDD;
	private static String N0;
	private static String E0;
	private static String NP0;
	private static String EP0;
	private static String N1;
	private static String E1;
	private static String NP1;
	private static String EP1;

	private static HashSet<String> availableRels;
	private static DatalogProgram program;

	private static Atom matchHeadAtom;
	private static HashSet<String> matchVarsToBeUsedInF;

	private static void initialize(String base, String view) {
		baseName = base;
		viewName = view;

		MAP = Config.relname_mapping + "_" + viewName;
		NDA = Config.relname_node + "_" + Config.relname_added + "_" + viewName;
		NDD = Config.relname_node + "_" + Config.relname_deleted + "_" + viewName;
		EDA = Config.relname_edge + "_" + Config.relname_added + "_" + viewName;
		EDD = Config.relname_edge + "_" + Config.relname_deleted + "_" + viewName;
		N0 = Config.relname_node + "_" + baseName;
		E0 = Config.relname_edge + "_" + baseName;
		N1 = Config.relname_node + "_" + viewName;
		E1 = Config.relname_edge + "_" + viewName;
		NP0 = Config.relname_nodeprop + "_" + baseName;
		EP0 = Config.relname_edgeprop + "_" + baseName;
		NP1 = Config.relname_nodeprop + "_" + viewName;
		EP1 = Config.relname_edgeprop + "_" + viewName;		
		availableRels = new HashSet<String>();
		matchVarsToBeUsedInF = new LinkedHashSet<String>();
	}
	
	public static HashSet<String> getMatchVarsToBeUsedInF() {
		return matchVarsToBeUsedInF;
	}

	private static boolean checkIncludedAtom(TransRule transRule, Atom a, boolean useWhereClause) {
		if (a.getPredicate().isInterpreted() == true && useWhereClause == false) {
			if (transRule.getVarsInWhereClause().contains(a.getTerms().get(0).getVar()) == true) {
				return false;
			}
		}
		return true;
	}

	public static void insertCatalogView(Store store, String name, String base, String type, String query, long level) {
		ArrayList<SimpleTerm> args = new ArrayList<SimpleTerm>();
		args.add(new StringSimpleTerm(name));
		args.add(new StringSimpleTerm(base));
		args.add(new StringSimpleTerm(type));
		args.add(new StringSimpleTerm(Util.addSlashes(query)));
		args.add(new LongSimpleTerm(level));
		store.addTuple(Config.relname_catalog_view, args);
	}

	public static void addViewRuleToProgram(DatalogProgram p, 
			TransRuleList transRuleList, boolean isAllIncluded, boolean usewhereClause) {
		addViewRuleToProgram(p, transRuleList, isAllIncluded, -1, usewhereClause);
	}

	public static void addMatchRule(TransRuleList rules, int index, boolean useWhereClause) {
		matchVarsToBeUsedInF.clear();
		TransRule tr = rules.getTransRuleList().get(index);
		HashSet<String> headVars = new LinkedHashSet<String>();
		for (Atom a : tr.getPatternMatch()) {
			if (useWhereClause == false) {
				if (a.getPredicate().getRelName().contentEquals(Config.relname_edgeprop) == true ||
						a.getPredicate().getRelName().contentEquals(Config.relname_nodeprop) == true) {
					continue;
				}
				if (a.isInterpreted() == true && tr.getVarsInWhereClause().contains(a.getTerms().get(0).getVar()) == true) {
					continue;
				}
			}
			headVars.addAll(a.getVars());
		}

		String headRel = Config.relname_match + "_" + rules.getViewName() + "_" + index;
		DatalogClause c = new DatalogClause();
		Atom head = new Atom(headRel, headVars);
		c.setHead(head);
		c.addAtomToHeads(head);
		
		for (int j = 0; j < tr.getPatternMatch().size(); j++) {
			Atom a = tr.getPatternMatch().get(j);
			if (checkIncludedAtom(tr, a, useWhereClause) == false) {
				continue;
			}
			if (useWhereClause == false) {
				if (a.getPredicate().getRelName().contentEquals(Config.relname_nodeprop) == true ||
						a.getPredicate().getRelName().contentEquals(Config.relname_edgeprop) == true) {
					continue;
				}
			}

			Atom b = null;
			if (a.isInterpreted() == false) {
				String relName = a.getPredicate().getRelName() + "_" + rules.getBaseName();
				try {
					b = (Atom)a.clone();
				} catch (CloneNotSupportedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				b.getPredicate().setRelName(relName);

				if (a.equals(b) == true) {
					System.out.println("[OMG] a.equals(b) is true");
				}				 

				if (a.getPredicate().equals(b.getPredicate()) == true) {
					System.out.println("[OMG] a.getPredicate().equals(b.getPredicate()) is True");
				}
				c.getBody().add(b);
			} else {
				c.getBody().add(a);
			}

			if (a.getPredicate().getRelName().contentEquals(Config.relname_node) == true ||
					a.getPredicate().getRelName().contentEquals(Config.relname_edge) == true) {
				matchVarsToBeUsedInF.addAll(a.getVars());
			}
		}
		matchHeadAtom = c.getHead();
		program.addRule(c);
		
		ArrayList<Integer> indexSet = null;
;
		for (int i = 0; i < c.getHead().getTerms().size(); i++) {
			indexSet = new ArrayList<Integer>();
			indexSet.add(i);
			program.addIndexSet(matchHeadAtom.getRelName(), indexSet);
		}
	}

	public static Atom getNewIdAtom(int rid, String var) {
		int len = matchVarsToBeUsedInF.size();
		if (len > 30) {
			throw new IllegalArgumentException("len[" + len + "] should be less than 30. (matchVarsToBeUsedInF: " + matchVarsToBeUsedInF);
		}
		String udf = Config.relname_gennewid + "_MAP_" + viewName + "_" + rid;
		Atom a = new Atom(new Predicate(udf)); // len
//		a.getTerms().add(new Term(Integer.toString(rid), false));
		for (String v : matchVarsToBeUsedInF ){
			a.getTerms().add(new Term(v, true));
		}
		a.getTerms().add(new Term(var, true));
		program.getUDFs().add(udf);
		program.getEDBs().add(udf);

		return a;
	}

	public static void addMapRules(TransRuleList rules, int index, boolean useWhereClause) {
		TransRule tr = rules.getTransRuleList().get(index);
		for (HashMap.Entry<Atom, HashSet<String>> entry : tr.getMapMap().entrySet()) {
			String dstVar = entry.getKey().getTerms().get(0).toString();
			String dstLabel = entry.getKey().getTerms().get(1).toString();

			rid++;
			
			ArrayList<DatalogClause> cs = new ArrayList<DatalogClause>();
			DatalogClause c = null;
			Atom head = null;
			Atom b = null;
			for (String srcVar : entry.getValue()) {
				c = new DatalogClause();
				String srcVarLabel = null;
				String srcLabel = null;

				for (Atom a : tr.getPatternMatch()) {
					if (checkIncludedAtom(tr, a, useWhereClause) == false) {
						continue;
					}
					if (a.getTerms().get(0).getVar().contentEquals(srcVar) == true) {
						if (a.getTerms().get(1).isConstant() == true) {
							String label = a.getTerms().get(1).toString();
							srcVarLabel = "__label";
							srcLabel = label;
						} else {
							srcLabel = a.getTerms().get(0).toString() + "_label";
							srcVarLabel = a.getTerms().get(0).toString() + "_label";
						}
						break;
					}
				}
				if (srcVarLabel == null) {
					throw new IllegalArgumentException("srcVarLabel is null. srcVar: " + srcVar + ", transRule.getMapMap(): " + tr.getMapMap());
				}

				head = new Atom(MAP);
				head.appendTerm(new Term(srcVar, true));
				head.appendTerm(new Term(srcLabel, false));
				head.appendTerm(new Term(dstVar, true));
				head.appendTerm(new Term(dstLabel, false));
//				c.setHead(head);
				c.addAtomToHeads(head);
				c.getBody().add(matchHeadAtom);

				b = getNewIdAtom(rid, dstVar);
				c.getBody().add(b);
				cs.add(c);
			}

//			c = new DatalogClause();
//			head = new Atom(NP1);
//			head.appendTerm(new Term("dst", true));
//			head.appendTerm(new Term("key", true));
//			head.appendTerm(new Term("value", true));
//			c.addAtomToHeads(head);
//			b = new Atom(NP0);
//			b.appendTerm(new Term("src", true));
//			b.appendTerm(new Term("key", true));
//			b.appendTerm(new Term("value", true));
//			c.addAtomToBody(b);
//			b = new Atom(MAP);
//			b.appendTerm(new Term("src", true));
//			b.appendTerm(new Term("_", true));
//			b.appendTerm(new Term("dst", true));
//			b.appendTerm(new Term("_", true));
//			c.addAtomToBody(b);
//			cs.add(c);
//
//			c = new DatalogClause();
//			head = new Atom(NP1);
//			head.appendTerm(new Term("n", true));
//			head.appendTerm(new Term("key", true));
//			head.appendTerm(new Term("value", true));
//			c.addAtomToHeads(head);
//			b = new Atom(NP0);
//			b.appendTerm(new Term("n", true));
//			b.appendTerm(new Term("key", true));
//			b.appendTerm(new Term("value", true));
//			c.addAtomToBody(b);
//			b = new Atom(NDD);
//			b.setNegated(true);
//			b.appendTerm(new Term("n", true));
//			b.appendTerm(new Term("_", true));
//			c.addAtomToBody(b);
//			
//			cs.add(c);
			
			availableRels.add("MAP");
			availableRels.add("NDA");
			availableRels.add("NDD");
			availableRels.add("EDA");
			availableRels.add("EDD");

//			if (Config.isLogicBlox() == true) {
				program.addConstructorForLB(viewName, rid, matchVarsToBeUsedInF.size());

				DatalogClause c1 = new DatalogClause();
				Atom a1 = new Atom(Config.relname_gennewid + "_CONST_" + viewName + "_" + rid);
				for (String v : matchVarsToBeUsedInF) {
					a1.appendTerm(new Term(v, true));					
				}
				a1.appendTerm(new Term("_v", true));
				c1.addAtomToHeads(a1);
				a1 = new Atom(Config.relname_gennewid + "_" + viewName);
				a1.appendTerm(new Term("_v", true));
				c1.addAtomToHeads(a1);
				c1.addAtomToBody(matchHeadAtom);
				program.addRule(c1);
				
//				System.out.println("cccc1: " + c1);
//			}
			
			for (DatalogClause c2 : cs) {
				program.addRule(c2);
			}
		}		
	}

	public enum OPTION {
		ADD, REMOVE
	};

	public static void addAddRemoveRules(TransRuleList rules, int index, boolean useWhereClause) {
		TransRule tr = rules.getTransRuleList().get(index);

		HashMap<String, Integer> metaMap = new HashMap<String, Integer>();
		HashSet<String> newNodes = new HashSet<String>();
		HashSet<String> newEdges = new HashSet<String>();

		//		DatalogClause clause_use = new DatalogClause();
		//		DatalogClause clause_const = new DatalogClause();

		//		HashSet<String> vars = new HashSet<String>();
		ArrayList<Atom> atoms = null;

		for (OPTION option : OPTION.values()) {
			if (option == OPTION.ADD) {
				atoms = tr.getPatternAdd();
			} else if (option == OPTION.REMOVE) {
				atoms = tr.getPatternRemove();
			}
			for (int j = 0; j < atoms.size(); j++) {
				Atom a = atoms.get(j);
				rid++;

				Atom head = null;
				String predRel = null;
				String var = a.getTerms().get(0).getVar();
				String label = null;

				if (a.getPredicate().equals(Config.predN)) {
					label = a.getTerms().get(1).toString();

					if (option == OPTION.ADD) {
						predRel = NDA;
						newNodes.add(var);
						metaMap.put(var, rid);
						availableRels.add("NDA");
					} else if (option == OPTION.REMOVE) {
						// FIXME: we currently assume that we remove non-meta node only (level is also specified)
						predRel = NDD;
						availableRels.add("NDD");
					}
					head = new Atom(predRel, var);
				} else if (a.getPredicate().equals(Config.predE)) {
					String from = a.getTerms().get(1).toString();
					String to = a.getTerms().get(2).toString();
					label = a.getTerms().get(3).toString();

					if (option == OPTION.ADD) {
						predRel = EDA;						
						newEdges.add(var);
						availableRels.add("EDA");
					} else if (option == OPTION.REMOVE) {
						predRel = EDD;
						availableRels.add("EDD");
					}
					head = new Atom(predRel, var, from, to);
				}

				DatalogClause c = new DatalogClause();				
				head.appendTerm(new Term(label, false));
				c.setHead(head);;

				Atom b = getNewIdAtom(rid, var);
				c.getBody().add(matchHeadAtom);
				c.getBody().add(b);

//				if (Config.isLogicBlox() == true) {
					program.addConstructorForLB(viewName, rid, matchVarsToBeUsedInF.size());
//				}
				//				clause_use.addAtomToHeads(head);
					
				// Add constructor rule
				DatalogClause c1 = new DatalogClause();
				Atom a1 = new Atom(Config.relname_gennewid + "_CONST_" + viewName + "_" + rid);
				for (String v : matchVarsToBeUsedInF) {
					a1.appendTerm(new Term(v, true));					
				}
				a1.appendTerm(new Term("_v", true));
				c1.addAtomToHeads(a1);
				a1 = new Atom(Config.relname_gennewid + "_" + viewName);
				a1.appendTerm(new Term("_v", true));
				c1.addAtomToHeads(a1);
				c1.addAtomToBody(matchHeadAtom);
				program.addRule(c1);
					
//				System.out.println("c: " + c);
				program.addRule(c);
			}
		}

		//		Atom head = new Atom(new Predicate("MATCH_" + viewName + "_" + index));
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
		//		ArrayList<Atom> heads = new ArrayList<Atom>();
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
		//			}
		//		}
		//
		//		for (int i = 0; i < heads.size(); i++) {
		//			clause_use.addAtomToHeads(heads.get(i));
		//		}
		//		
		//		System.out.println("[addDeltaRules] clause_const: " + clause_const);
		//		System.out.println("[addDeltaRules] clause_use: " + clause_use);
		//		
		//		program.addEDB(head.getPredicate().getRelName());
		//		program.addRule(clause_const);
		//		program.addRule(clause_use);
	}	

	public static void addDeltaRules()  {
		DatalogClause c = null;

		if (availableRels.contains("MAP") == true) {
			// add node
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(NDA, "id", "label"));
			c.addAtomToBody(new Atom(MAP, "_", "_", "id", "label"));
			program.addRule(c);
			// remove node
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(NDD, "m", "m_label"));
			c.addAtomToBody(new Atom(MAP, "m", "m_label", "_", "_"));
			program.addRule(c);
	
			// deltas for edge
			// add edge
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(EDA, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(E0, "id", "_from", "to", "label"));
			c.addAtomToBody(new Atom(MAP, "_from", "_", "from", "_"));
			c.addAtomToBody(new Atom(false, MAP, "to", "_", "_", "_"));
			program.addRule(c);
	
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(EDA, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(E0, "id", "from", "_to", "label"));
			c.addAtomToBody(new Atom(false, MAP, "from", "_", "_", "_"));
			c.addAtomToBody(new Atom(MAP, "_to", "_", "to", "_"));
			program.addRule(c);
	
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(EDA, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(E0, "id", "_from", "_to", "label"));
			c.addAtomToBody(new Atom(MAP, "_from", "_", "from", "_"));
			c.addAtomToBody(new Atom(MAP, "_to", "_", "to", "_"));
			program.addRule(c);
			
			availableRels.add(NDA);
			availableRels.add(NDD);
			availableRels.add(EDA);	
		}
		
		if (availableRels.contains("NDD") == true) {
	 		// remove edge
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(EDD, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(E0, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(NDD, "from", "_"));
			program.addRule(c);
	
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(EDD, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(E0, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(NDD, "to", "_"));
			program.addRule(c);		
			
			availableRels.add(EDD);
		}
	}	

	public static void addViewRules() {
		DatalogClause c = new DatalogClause();
		c.addAtomToHeads(new Atom(N1, "id", "label"));
		c.addAtomToBody(new Atom(N0, "id", "label"));
		if (availableRels.contains("NDD") == true) {
			c.addAtomToBody(new Atom(false, NDD, "id", "_"));
		}
		program.addRule(c);

		if (availableRels.contains("NDA") == true) {
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(N1, "id", "label"));
			c.addAtomToBody(new Atom(NDA, "id", "label"));
			program.addRule(c);
		}
		c = new DatalogClause();
		c.addAtomToHeads(new Atom(E1, "id", "from", "to", "label"));
		c.addAtomToBody(new Atom(E0, "id", "from", "to", "label"));
		if (availableRels.contains("EDD") == true) {
			c.addAtomToBody(new Atom(false, EDD, "id", "_", "_", "_"));
		}
		program.addRule(c);

		if (availableRels.contains("EDA") == true) {
			c = new DatalogClause();
			c.addAtomToHeads(new Atom(E1, "id", "from", "to", "label"));
			c.addAtomToBody(new Atom(EDA, "id", "from", "to", "label"));
			program.addRule(c);
		}
	}

	public static void addViewRulesForSingleRule(TransRuleList rules, boolean isAllIncluded, int index, boolean useWhereClause) {		
		//		System.out.println("[addViewRulesForSingleRule] index: " + index + " useWhereClause: " + useWhereClause);
		String type = rules.getViewType();
		
		if (type.equals("materialized") == true) {
			addMatchRule(rules, index, useWhereClause);
			addMapRules(rules, index, useWhereClause);
			addAddRemoveRules(rules, index, useWhereClause);
			if (rules.getNumTransRuleList() == index + 1) {
				addDeltaRules();
				addPropertyRules();
			}
		} else if (isAllIncluded == true) {
			addMatchRule(rules, index, useWhereClause);
			addMapRules(rules, index, useWhereClause);
			addAddRemoveRules(rules, index, useWhereClause);
			addDeltaRules();
			addPropertyRules();
		} else {
			if (type.equals("asr") == true) {
				addMatchRule(rules, index, useWhereClause);
			} else if (type.equals("hybrid") == true) {
				addMatchRule(rules, index, useWhereClause);
				addMapRules(rules, index, useWhereClause);
//				addAddRemoveRules(rules, index, useWhereClause);
			}
		}
	}

	private static void addPropertyRules() {
		// TODO Auto-generated method stub
		DatalogClause c = new DatalogClause();
		Atom head;
		Atom b;
		
		if (availableRels.contains("MAP") == true) {
			head = new Atom(NP1);
			head.appendTerm(new Term("dst", true));
			head.appendTerm(new Term("key", true));
			head.appendTerm(new Term("value", true));
			c.addAtomToHeads(head);
			b = new Atom(NP0);
			b.appendTerm(new Term("src", true));
			b.appendTerm(new Term("key", true));
			b.appendTerm(new Term("value", true));
			c.addAtomToBody(b);
			b = new Atom(MAP);
			b.appendTerm(new Term("src", true));
			b.appendTerm(new Term("_", true));
			b.appendTerm(new Term("dst", true));
			b.appendTerm(new Term("_", true));
			c.addAtomToBody(b);
			program.addRule(c);
		}

		c = new DatalogClause();
		head = new Atom(NP1);
		head.appendTerm(new Term("n", true));
		head.appendTerm(new Term("key", true));
		head.appendTerm(new Term("value", true));
		c.addAtomToHeads(head);
		b = new Atom(NP0);
		b.appendTerm(new Term("n", true));
		b.appendTerm(new Term("key", true));
		b.appendTerm(new Term("value", true));
		c.addAtomToBody(b);
		if (availableRels.contains("NDD") == true) {
			b = new Atom(NDD);
			b.setNegated(true);
			b.appendTerm(new Term("n", true));
			b.appendTerm(new Term("_", true));
			c.addAtomToBody(b);
		}
		program.addRule(c);
		
		availableRels.add("NP1");
		availableRels.add("EP1");
	}

	/**
	 * @param p
	 * @param rules
	 * @param isAllIncluded True if all rules should be included (e.g., for rewriting)
	 * @param indexOfRule
	 * @param useWhereClause
	 */
	public static void addViewRuleToProgram(DatalogProgram p, TransRuleList rules, 
			boolean isAllIncluded, int indexOfRule, boolean useWhereClause) {
//		System.out.println("[addViewRuleToProgram] type: " + rules.getViewType() + " isAllInc: " + isAllIncluded);

		rid = 0;
		program = p;
		initialize(rules.getBaseName(), rules.getViewName());

		if (indexOfRule >= 0) { // for single rule only
			addViewRulesForSingleRule(rules, isAllIncluded, indexOfRule, useWhereClause);
		} else { // for all rules
			for (int i = 0; i < rules.getTransRuleList().size(); i++) {
				addViewRulesForSingleRule(rules, isAllIncluded, i, useWhereClause);
			}
		}

		if (rules.getViewType().equals("materialized") == true) {
			addViewRules();
		} else {
			if ((isAllIncluded == true && rules.getViewType().contentEquals("materialized") == false) ||
					(isAllIncluded == false && rules.getViewType().contentEquals("materialized") == true)) {
				addViewRules();
			}		
		}
	}
}