//package edu.upenn.cis.db.graphtrans.graphdb.datalog;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedHashSet;
//
//import org.apache.commons.lang3.tuple.Triple;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import edu.upenn.cis.db.ConjunctiveQuery.Atom;
//import edu.upenn.cis.db.ConjunctiveQuery.Predicate;
//import edu.upenn.cis.db.ConjunctiveQuery.Term;
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
//public class ViewRuleGen {
//	final static Logger logger = LogManager.getLogger(ViewRuleGen.class);
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
//	private static String NP0;
//	private static String EP0;
//	private static String N1;
//	private static String E1;
//	private static String NP1;
//	private static String EP1;
//	private static HashSet<String> headVars;
//	
//	private static HashSet<String> availableRels;
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
//		NP0 = Config.relname_nodeprop + "_" + baseName;
//		EP0 = Config.relname_edgeprop + "_" + baseName;
//		NP1 = Config.relname_nodeprop + "_" + viewName;
//		EP1 = Config.relname_edgeprop + "_" + viewName;		
//		availableRels = new HashSet<String>();
//	}
//	
//	private static String getDeltasFromMappigs() {
//		StringBuilder rule = new StringBuilder("# Delta rules\n");
//
//		// deltas for node
//		if (availableRels.contains("MAP") == true) {
//			rule.append("# add node\n")
//				.append(NDA).append("(id, id_l, id_r, label) <- ")
//				.append(MAP).append("(_u1, _u2, _u3, _u4, id, id_l, id_r, label).\n")
//				.append("# remove node\n")
//				.append(NDD).append("(m, m_l, m_r, m_label) <- ")
//				.append(MAP).append("(m, m_l, m_r, m_label, _u1, _u2, _u3, _u4).\n");
//		}
//		
//		// deltas for edge
//		if (availableRels.contains("MAP") == true) {
//			rule.append("# add edge\n")
//				.append(EDA).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(E0).append("(id, id_l, id_r, _from, _from_l, _from_r, to, to_l, to_r, label), ")
//				.append(MAP).append("(_from, _from_l, _from_r, _u1, from, from_l, from_r, _u2), ")
//				.append("!").append(MAP).append("(to, to_l, to_r, _u3, _u4, _u5, _u6, _u7).\n");
//			rule.append(EDA).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(E0).append("(id, id_l, id_r, from, from_l, from_r, _to, _to_l, _to_r, label), ")
//				.append("!").append(MAP).append("(from, from_l, from_r, _u1, _u2, _u3, _u4, _u5), ")
//				.append(MAP).append("(_to, _to_l, _to_r, _u6, to, to_l, to_r, _u7).\n");
//			rule.append(EDA).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(E0).append("(id, id_l, id_r, _from, _from_l, _from_r, _to, _to_l, _to_r, label), ")
//				.append(MAP).append("(_from, _from_l, _from_r, _u1, from, from_l, from_r, _u2), ")
//				.append(MAP).append("(_to, _to_l, _to_r, _u3, to, to_l, to_r, _u4).\n");
//		}
//		
//		if (availableRels.contains("NDD") == true) {
//			rule.append("# remove edge\n")
//				.append(EDD).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(E0).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label), ")
//				.append(NDD).append("(from, from_l, from_r, _u1).\n");
//			rule.append(EDD).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(E0).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label), ")
//				.append(NDD).append("(to, to_l, to_r, _u1).\n");
//		}
//		return rule.toString();
//	}
//		
//	/**
//	 * Construct view based on base graph and deltas
//	 * 
//	 * @param viewName
//	 * @param baseName
//	 * @return
//	 */	
//	public static String getViewFromBaseAndDeltasRule(String viewName, String baseName, boolean useWhereClause) {
//		StringBuilder rule = new StringBuilder();
//
//		rule.append("# Construct view from base and deltas\n");
//		
//		rule.append("# View construction rules\n");
//		
//		rule.append(N1).append("(id, id_l, id_r, label) <- ")
//			.append(N0).append("(id, id_l, id_r, label)");
//		if (availableRels.contains("NDD") == true) {
//			rule.append(", !").append(NDD).append("(id, id_l, id_r, _u1)");
//		}
//		rule.append(".\n");
//		if (availableRels.contains("NDA") == true) {
//			rule.append(N1).append("(id, id_l, id_r, label) <- ")
//				.append(NDA).append("(id, id_l, id_r, label).\n");
//		}
//		rule.append(E1).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//			.append(E0).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label)");
//		if (availableRels.contains("EDD") == true) {
//			rule.append(", !").append(EDD).append("(id, id_l, id_r, _u1, _u2, _u3, _u4, _u5, _u6, _u7)");
//		}
//		rule.append(".\n");
//		
//		if (availableRels.contains("EDA") == true) {
//			rule.append(E1).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label) <- ")
//				.append(EDA).append("(id, id_l, id_r, from, from_l, from_r, to, to_l, to_r, label).\n");
//		}
//
//		if (useWhereClause == true) {
//			rule.append("# Property construction rules\n");
//			if (availableRels.contains("MAP") == true) {
//				rule.append(NP1).append("(id, id_l, id_r, key, value) <- ")
//					.append(NP0).append("(from, from_l, from_r, key, value), ")
//					.append(MAP).append("(from, from_l, from_r, _u1, id, id_l, id_r, _u2).\n");
//			}
//			rule.append(NP1).append("(id, id_l, id_r, key, value) <- ")
//				.append(NP0).append("(id, id_l, id_r, key, value)");
//			if (availableRels.contains("NDD") == true) {
//				rule.append(", !").append(NDD).append("(id, id_l, id_r, _u1).\n");			
//			} else {
//				rule.append(".\n");
//			}
//			rule.append(EP1).append("(id, id_l, id_r, key, value) <- ")
//				.append(EP0).append("(id, id_l, id_r, key, value)");
//			rule.append(".\n");
//		}
//		return rule.toString();
//	}
//
//	private static String getViewFromBaseAndDeltasRule(boolean useWhereClause) {
//		return getViewFromBaseAndDeltasRule(viewName, baseName, useWhereClause);
//	}
//	
//	private static String getAtomOfBase(Atom atom) {
//		ArrayList<Atom> atoms = atom.getAtomBodyStrWithInterpretedAtoms(baseName);		
//		String str = atoms.get(0).toString();
//		headVars.addAll(atoms.get(0).getVars());
//		if (atoms.size() > 1) {
//			str += "," + atoms.get(1).toString();
//			headVars.add(atoms.get(1).getTerms().get(0).getVar());
//		}
//		return str;
//	}
//	
//	private static String getSingleTransRule(TransRule transRule, int indexOfRule) {
//		return getSingleTransRule(transRule, true, indexOfRule);
//	}
//	
//	private static boolean checkIncludedAtom(TransRule transRule, Atom a, boolean useWhereClause) {
//		if (a.getPredicate().isInterpreted() == true && useWhereClause == false) {
//			if (transRule.getVarsInWhereClause().contains(a.getTerms().get(0).getVar()) == true) {
//				return false;
//			}
//		}
//		return true;
//	}
//
//	private static String getSingleTransRule(TransRule transRule, Boolean useWhereClause, int indexOfRule) {
//		String rule = "# Single Trans Rule\n";
//		rid = indexOfRule * 100; // Assume each rule has less than 100 subrules
//		
//		/**
//		 * 1. Construct RHS
//		 * 2. Map
//		 * 3. Add/remove
//		 */
//		String body = "";
//		String var_rep = null; // representative var
//		HashMap<String, Integer> metaMap = new HashMap<String, Integer>();
//		
//		// create body
//		body += transRule.getHeadMatch();
//		
//		for (int i = 0; i < transRule.getPatternMatch().size(); i++) {
//			Atom a = transRule.getPatternMatch().get(i);
//			if (checkIncludedAtom(transRule, a, useWhereClause) == false) {
//				continue;
//			}
//
//			if (var_rep == null) {
//				if (transRule.getPatternMatch().get(i).getPredicate().equals(Config.predN_w) == true) {
//					String var_rep_candidate = transRule.getPatternMatch().get(i).getTerms().get(0).getVar();
//					if (transRule.getStarVarSet().contains(var_rep_candidate) == false) {
//						var_rep = var_rep_candidate;
//					}
//				}
//			}			
//		}
//		
//		// mapping rules
//		rule += "# Mapping Rules\n";
//		
//		for (HashMap.Entry<Atom, HashSet<String>> entry : transRule.getMapMap().entrySet()) {
//			String dstVar = entry.getKey().getTerms().get(0).toString();
//			String dstLabel = entry.getKey().getTerms().get(1).toString();
//			
//			rid++;
//
//			for (String srcVar : entry.getValue()) {
//				String extraRule = "";
//				String srcVarLabel = null;
//				
//				for (Atom a : transRule.getPatternMatch()) {
//					if (checkIncludedAtom(transRule, a, useWhereClause) == false) {
//						continue;
//					}
//					if (a.getTerms().get(0).getVar().contentEquals(srcVar) == true) {
//						if (a.getTerms().get(3).isConstant() == true) {
//							String label = a.getTerms().get(3).toString(); //a.getInterpreted().get(3);
//							srcVarLabel = "__label";
//							extraRule = ", " + srcVarLabel + "=" + label; 
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
//				rule += Config.relname_mapping + "_" + viewName 
//						+ "(" + srcVar + ", " + srcVar + "_l, " + srcVar + "_r" + ", " + srcVarLabel + ", " // source (members)
//						+ var_rep + ", " + "_x_l, _x_r, label" // target (super node)
//						+ ") <- " + 
//						body + 
//						", _x_l = " + transRule.getLevel() + 
//						", _x_r = " + rid + 
//						", label = " + dstLabel +  
//						extraRule +
//						".\n";
//								
//				metaMap.put(dstVar, rid);
//				
//				transRule.getMetaNodeMap().put(dstVar, Triple.of(var_rep, (int)transRule.getLevel(), rid));
//				
//				availableRels.add("MAP");
//				availableRels.add("NDA");
//				availableRels.add("NDD");
//				availableRels.add("EDA");
//				availableRels.add("EDD");
//			}
//		}
//		
//		rule += "# add node/edge\n";
//		for (int i = 0; i < transRule.getPatternAdd().size(); i++) {
//			Atom a = transRule.getPatternAdd().get(i);
//			rid++;
//			if (a.getPredicate().equals(Config.predN)) { // add a new node
//				String extraRule = "";
//				rule += NDA + "(" + var_rep + ", vvn0, vvn1, " +
//						a.getTerms().get(1).toString() + ") <- " + body;
//
//				String var = a.getTerms().get(0).getVar();
//				transRule.getMetaNodeMap().put(var, Triple.of(var_rep, (int)transRule.getLevel(), rid));
//				System.out.println("[getSingleTransRule] addNode var: " + var + " -> " + var_rep + "," +transRule.getLevel() + "," + rid);
//
//				extraRule += ", vvn0 = " + transRule.getLevel() + ", vvn1 = " + rid;
//				rule += extraRule + ".\n";
//				
//				transRule.getMetaNodeMap().put(var, Triple.of(var_rep, (int)transRule.getLevel(), rid));
//				
//				metaMap.put(a.getTerms().get(0).toString(), rid);
//				availableRels.add("NDA");	
//			} else if (a.getPredicate().equals(Config.predE)) { // add a new edge
//				String extraRule = "";
//				rule += EDA + "(" + var_rep + ", vv0, vv1, ";
//				extraRule += ", vv0 = " + transRule.getLevel() + ", vv1 = " + rid;
//				
//				String var = a.getTerms().get(0).getVar();
//				String from = a.getTerms().get(1).toString();
//				String to = a.getTerms().get(2).toString();
//				String label = a.getTerms().get(3).toString();
//
//				transRule.getMetaNodeMap().put(var, Triple.of(var_rep, (int)transRule.getLevel(), rid));
//				
//				if (metaMap.containsKey(from) == true) { // meta
//					rule += var_rep + ", vv2, vv3, ";
//					extraRule += ", vv2 = " + transRule.getLevel() + ", vv3 = " + metaMap.get(from);
//				} else {
//					rule += from + ", " + from + "_l, " + from + "_r, ";
//				}
//				
//				if (metaMap.containsKey(to) == true) { // meta
//					rule += var_rep + ", " + transRule.getLevel() + ", " + metaMap.get(to) + ", ";
//					rule += var_rep + ", vv4, vv5, ";
//					extraRule += ", vv4 = " + transRule.getLevel() + ", vv5 = " + metaMap.get(from);
//				} else {
//					rule += to + ", " + to + "_l, " + to + "_r, ";
//				}
//				rule += label + ") <- " + body;
//				if (extraRule.contentEquals("") == false) {
//					rule += extraRule;
//				}
//				rule += ".\n";
//				availableRels.add("EDA");				
//			}
//		}		
//		rule += "# remove node/edge\n";
//		for (int i = 0; i < transRule.getPatternRemove().size(); i++) {
//			Atom a = transRule.getPatternRemove().get(i);
//
//			if (a.getPredicate().equals(Config.predN)) {
//				// FIXME: we currently assume that we remove non-meta node only (level is also specified)
//				String var = a.getTerms().get(0).toString();
//				String label = a.getTerms().get(1).toString();
//				rule += NDD + "(" + var + ", " + var + "_l, " + var + "_r, " + label + ") <- " + body + 
//						".\n";				
//				availableRels.add("NDD");				
//			} else if (a.getPredicate().equals(Config.predE)) {
//				String var = a.getTerms().get(0).toString();
//				String from = a.getTerms().get(1).toString();
//				String to = a.getTerms().get(2).toString();
//				String label = a.getTerms().get(3).toString();
//				
//				rule += EDD + "(" + var + ", " + var + "_l, " + var + "_r, " +
//							from + ", " + from + "_l, " + from + "_r, " +
//							to + ", " + to + "_l, " + to + "_r, " +
//							label + ") <- " + body + ".\n";
//				availableRels.add("EDD");
//			}
//		}
//		return rule;
//	}
//
//	public static String getRule(TransRuleList transRuleList, boolean isAllIncluded) {
//		return getRule(transRuleList, isAllIncluded, -1, true);
//	}
//	
//	public static String getRule(TransRuleList transRuleList, boolean isAllIncluded, int indexOfRule, boolean useWhereClause) {
//		String viewType = transRuleList.getViewType();
//		StringBuilder rule = new StringBuilder("# View Rule\n");
//		initialize(transRuleList.getBaseName(), transRuleList.getViewName());
//		
//		if (isAllIncluded == true || viewType.contentEquals("virtual") == false) {
//			for (int i = 0; i < transRuleList.getNumTransRuleList(); i++) {
//				if (indexOfRule >= 0) {
//					i = indexOfRule;
//				}
//				TransRule tr = transRuleList.getTransRuleList().get(i);
//
//				HashSet<String> headVars = new LinkedHashSet<String>(); 
//				for (Atom a : tr.getPatternMatch()) {
//					if (useWhereClause == false) {
//						if (a.getPredicate().getRelName().contentEquals(Config.relname_edgeprop) == true ||
//								a.getPredicate().getRelName().contentEquals(Config.relname_nodeprop) == true) {
//							continue;
//						}
//						if (a.isInterpreted() == true && tr.getVarsInWhereClause().contains(a.getTerms().get(0).getVar()) == true) {
//							continue;
//						}
//					}
//					headVars.addAll(a.getVars());
//				}
//				String headVarsForMatch = "";
//				for (String var : headVars) {
//					if (headVarsForMatch.contentEquals("") == false) {
//						headVarsForMatch += ",";
//					}
//					headVarsForMatch += var;
//				}
//			
//				String headMatch = Config.relname_match + "_" + transRuleList.getViewName() + "_" + i + "(" + headVarsForMatch + ")";
//				rule.append(headMatch).append(" <- ");
//				tr.setHeadMatch(headMatch);
//								
//				for (int j = 0; j < tr.getPatternMatch().size(); j++) {
//					Atom a = tr.getPatternMatch().get(j);
//					if (checkIncludedAtom(tr, a, useWhereClause) == false) {
//						continue;
//					}
//					if (useWhereClause == false) {
//						if (a.getPredicate().getRelName().contentEquals(Config.relname_nodeprop) == true ||
//							a.getPredicate().getRelName().contentEquals(Config.relname_edgeprop) == true) {
//							continue;
//						}
//					}
//					if (j > 0) {
//						rule.append(",");
//					}
//					rule.append(getAtomOfBase(a));
//				}
//				rule.append(".\n");
//				
//				if (isAllIncluded == true || viewType.contentEquals("asr") == false) {
//					rule.append(getSingleTransRule(transRuleList.getTransRule(i), useWhereClause, i));
//					
//				}
//				
//				if (indexOfRule >= 0) {
//					break;
//				}
//			}
//			if (isAllIncluded == true || viewType.contentEquals("asr") == false) {
//				rule.append(getDeltasFromMappigs());
//			}
//		}
//		if (isAllIncluded == true || viewType.contentEquals("materialized") == true) {
//			rule.append(getViewFromBaseAndDeltasRule(useWhereClause));
//		}		
//		return rule.toString();
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