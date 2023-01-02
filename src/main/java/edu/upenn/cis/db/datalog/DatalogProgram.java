package edu.upenn.cis.db.datalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.logicblox.connect.ConnectBlox.Response;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.ConjunctiveQuery.Predicate;
import edu.upenn.cis.db.ConjunctiveQuery.Term;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.helper.Util;
import edu.upenn.cis.db.logicblox.LogicBlox;

/**
 * Datalog Program
 * 
 * @author sbnet21
 *
 */
public class DatalogProgram {
	final static Logger logger = LogManager.getLogger(DatalogProgram.class);
	
	private Map<String, Predicate> predicates;
	private LinkedList<String> headRules;
	private Map<String, List<DatalogClause>> rules;
	private Set<String> EDBs;
	private Set<String> IDBs;
	private Set<String> UDFs;
	private Set<String> materializedIDBs;
	private int ruleCount = 0;
	private int createdViewId = 0;
	
	private HashMap<String, HashSet<Integer>> EDBToGenIdMap;
//	private HashMap<String, Integer> NewGenConstInfo; // v0_1 |-> 3 inputs (there is a new id, so it has 4 values)
	
	public HashMap<String, HashSet<Integer>> getEDBToGenIdMap() {
		return EDBToGenIdMap;
	}

	public void setEDBToGenIdMap(HashMap<String, HashSet<Integer>> eDBToGenIdMap) {
		EDBToGenIdMap = eDBToGenIdMap;
	}

	// viewName -> (rid -> # of arguments in constructor) only for LogicBlox
	private HashMap<String, HashMap<Integer, Integer>> constructorForLB; 
	
	private Map<String, ArrayList<ArrayList<Integer>>> indexSets; // For postgres only
	
	public Atom mkAtom(String relName, String ... vars) {
		ArrayList<Term> terms = new ArrayList<Term>();
        for (String v : vars ){
        	terms.add(new Term(v, true));
        }	
        return new Atom(new Predicate(relName), terms);
	}
	
	public ArrayList<Atom> mkAtomList(Atom ...atoms) {
		ArrayList<Atom> atomSet = new ArrayList<Atom>();
		for (Atom a : atoms) {
			atomSet.add(a);
		}
		return atomSet;
	}
	
	public DatalogClause mkClause(ArrayList<Atom> head, ArrayList<Atom> body) {
		return new DatalogClause(head, body);
	}
	
	public void addIndexSet(String name, ArrayList<Integer> indexSet) {
		if (indexSets.containsKey(name) == false) {
			indexSets.put(name, new ArrayList<ArrayList<Integer>>());
		}
		indexSets.get(name).add(indexSet);
	}
	
	public ArrayList<ArrayList<Integer>> getIndexSet(String name) {
		return indexSets.get(name);
	}
	
	public DatalogProgram() {
		constructorForLB = new HashMap<String, HashMap<Integer, Integer>>();
		
		predicates = new HashMap<String, Predicate>(); 
		rules = new LinkedHashMap<String, List<DatalogClause>>();
		headRules = new LinkedList<String>();
		EDBs = new HashSet<String>();
		IDBs = new HashSet<String>();
		UDFs = new HashSet<String>();
		materializedIDBs = new HashSet<String>();
		indexSets = new HashMap<String, ArrayList<ArrayList<Integer>>>();
		EDBToGenIdMap = new HashMap<String, HashSet<Integer>>();
//		NewGenConstInfo = new HashMap<String, Integer>();
		
		EDBs.add(Config.relname_node + Config.relname_base_postfix);
		EDBs.add(Config.relname_edge + Config.relname_base_postfix);
		EDBs.add(Config.relname_nodeprop + Config.relname_base_postfix);
		EDBs.add(Config.relname_edgeprop + Config.relname_base_postfix);
	}
	
	public void addEDB(String e) {
//		System.out.println("addEDB e: " + e);
		EDBs.add(e);
	}

	public void addPredicate(Predicate p) {
		String rel_name = p.getRelName();
		predicates.put(rel_name, p);
	}
	
	public Predicate getPredicate(String r) {
		return predicates.get(r);
	}
	
	/**
	 * Run subqueries of the given relation name.
	 * @param relName
	 */
	public void runRule(String relName) {
		List<DatalogClause> rs = rules.get(relName);
		
		StringBuilder str = new StringBuilder();
		for (DatalogClause c : rs) {
			str.append(c);
			str.append(".\n");
		}
		Response res = LogicBlox.runAddBlock(Config.getWorkspace(), null, str.toString());
	}
	
	/**
	 * Check if the relation has 0-size.
	 * @param relName
	 * @return true if 0-size, otherwise false.
	 */
	public boolean checkZeroPred(String relName) {
		long count = LogicBlox.getPredicatePopcount(Config.getWorkspace(), "", relName);
//		System.out.println("relName: " + relName + " count: " + count);
		return (count == 0);
	}
	
	public void addRule(DatalogClause c) {
		if (c == null) return;

//		Util.console_logln("[addRule] c: " + c, 3);
		// validity check
		HashSet<String> headVars = c.getHead().getVars();
		HashSet<String> bodyVars = new HashSet<String>();
		ArrayList<Atom> atomsToDelete = new ArrayList<Atom>();
		for (Atom a : c.getBody()) {
			if (a.isInterpreted() == false) {
				bodyVars.addAll(a.getVars());
			}
		}
		for (Atom a : c.getBody()) {
//			if (a.getTerms().get(0).getVar().contentEquals("n1_name_val") == true) {
//				System.out.println("*****a: " + a + " isInterpreted: " + a.isInterpreted() + " isVar: " + a.getTerms().get(0).isVariable());
//			}
			if (a.isInterpreted() == true) {
				for (Term t : a.getTerms()) {
					if (t.isVariable() == true) {
						String var = t.getVar();
//						if (var.contentEquals("n1_name_val") == true) {
//							System.out.println("________________var: " + var);
//						}
						if (bodyVars.contains(var) == false && headVars.contains(var) == false) {
							atomsToDelete.add(a);
						}
					}
				}
			}
		}
		if (atomsToDelete.size() > 0) {
//			System.out.println("____________atomsToDelete: " + atomsToDelete);
//			System.out.println("atomsToDelete c: " + c);
			c.getBody().removeAll(atomsToDelete);
//			System.out.println("atomsToDelete after c: " + c);
		}
		

		
//		System.out.println("\taddRule: " + c);
		if (Config.isPostgres() == true) {
			if (c.getHeads().size() == 0) {
				c.addAtomToHeads(c.getHead());;	
			}			
			for (int i = 0; i < c.getHeads().size(); i++) {
				DatalogClause dc = new DatalogClause();
				dc.addAtomToHeads(c.getHeads().get(i));
				dc.getBody().addAll(c.getBody());
				String headPred = dc.getHeads().get(0).getPredicate().getRelName();
				if (rules.containsKey(headPred) == false) {
					headRules.addLast(headPred);
					rules.put(headPred, new ArrayList<DatalogClause>());
				} else { // contain
					headRules.remove(headPred);
					headRules.addLast(headPred);
				}
				rules.get(headPred).add(dc);
			}
		} else {
			String headPred;
			if (c.getHeads().size() > 1) {
				headPred = c.getHeads().get(0).getPredicate().getRelName();	
			} else {
				headPred = c.getHead().getPredicate().getRelName();
			}
			
			if (rules.containsKey(headPred) == false) {
				headRules.addLast(headPred);
				rules.put(headPred, new ArrayList<DatalogClause>());
			} else { // contain
				headRules.remove(headPred);
				headRules.addLast(headPred);
			}
			rules.get(headPred).add(c);
		}
		ruleCount++;
		
		for (Atom a : c.getBody()) {
			if (a.getRelName().startsWith(Config.relname_gennewid + "_") == true) {
				int lastIdx = a.getTerms().size() - 1;
				String genid = a.getTerms().get(lastIdx).getVar();
//				System.out.println("***************genid: " + genid + " a: " + a + " head: " + c.getHead());
				for (int i = 0; i < c.getHead().getTerms().size(); i++) {
					String headVar = c.getHead().getTerms().get(i).getVar();
					if (genid.contentEquals(headVar)) {
//						System.out.println("***************head: " + c.getHead() + " has genid: " + genid + " at " + i);
						if (EDBToGenIdMap.containsKey(c.getHead().getRelName()) == false) {
							EDBToGenIdMap.put(c.getHead().getRelName(), new HashSet<Integer>());
						}
						EDBToGenIdMap.get(c.getHead().getRelName()).add(i);
					}
				}
			} else if (EDBToGenIdMap.containsKey(a.getRelName()) == true) {
				for (int idx : EDBToGenIdMap.get(a.getRelName())) {
					for (int i = 0; i < c.getHead().getTerms().size(); i++) {
						if (EDBToGenIdMap.get(a.getRelName()).contains(i) == true) {
//							String var = a.getTerms().get(idx).getVar();
							if (EDBToGenIdMap.containsKey(c.getHead().getRelName()) == false) {
								EDBToGenIdMap.put(c.getHead().getRelName(), new HashSet<Integer>());
							}
							EDBToGenIdMap.get(c.getHead().getRelName()).add(i);
						}
					}
				}
			}
		}
		
	}
	
	public int getRuleSize() {
		return rules.size();
	}
	
	public int getRuleCount() {
		return ruleCount;
	}
		
	/**
	 * 
	 * @param p predicate name
	 */
	public List<DatalogClause> getRules(String p) {
		return rules.get(p);
	}
	
	public String getString() {
		return getString(false);
	}
	
	public String getString(boolean onlyConstructor) {
		StringBuilder str = new StringBuilder();
		for (String view : constructorForLB.keySet()) {
			String newnode = Config.relname_gennewid;// + "_" + view;
			HashMap<Integer, Integer> m = constructorForLB.get(view);

			str.append(newnode).append("_").append(view).append("(n), ").append(newnode).append("_ID_").append(view).append("(n : id) -> int(id).\n");
			str.append("lang:autoNumbered(`").append(newnode).append("_ID_").append(view).append(").\n");
			
			for (HashMap.Entry<Integer, Integer> entry : m.entrySet()) {
				str.append(newnode).append("_CONST_").append(view).append("_").append(entry.getKey()).append("[");
				StringBuilder args = new StringBuilder();
				for (int j = 0 ; j < entry.getValue(); j++) {
					if (j > 0) {
						args.append(",");
					}
					args.append("t").append(j+1);
				}
				
				str.append(args);
				str.append("] = n -> ");
				for (int j = 0 ; j < entry.getValue(); j++) {
					if (j > 0) {
						str.append(", ");
					}
					str.append("int(t").append(j+1).append(")");
				}
				str.append(", ").append(newnode).append("_").append(view).append("(n).\n");
				str.append("lang:constructor(`").append(newnode).append("_CONST_").append(view).append("_").append(entry.getKey()).append(").\n");
				str.append(newnode).append("_MAP_").append(view).append("_").append(entry.getKey()).append("(").append(args).append(",id)");
				str.append(" <- ").append(newnode).append("_ID_").append(view).append("(n : id), ");
				str.append(newnode).append("_CONST_").append(view).append("_").append(entry.getKey()).append("(").append(args).append(",n).\n");
			}
		}
		
		if (onlyConstructor == false) {
			for (HashMap.Entry<String, List<DatalogClause>> entry : rules.entrySet()) {
				for (DatalogClause c : entry.getValue()) {
					str.append(c);
					str.append(".\n");
				}
			}
		}
		
//		System.out.println("str: " + str);
		return str.toString();
	}
	
	private void printRuleSubTree(Atom atom, int depth) {	
		List<DatalogClause> cs = rules.get(atom.getPredicate().getRelName());
		
		for (DatalogClause c : cs) {
			for (int i = 0; i < depth; i++) {
				System.out.print("    ");
			}
			System.out.println("+[depth: " + depth + " ==> " + c);

			for (Atom b : c.getBody()) {
				if (b.isInterpreted() == false && EDBs.contains(b.getPredicate().getRelName()) == false) {
					printRuleSubTree(b, depth+1);
				}
			}
		}
	}
	
	public void printRuleTree(DatalogClause c) {		
		System.out.println("====printRuleTree==== (START)");
		
		Set<String> head = new HashSet<String>();
		
		for (Atom a : c.getHeads()) {
			String rel = a.getPredicate().getRelName();
			if (head.contains(rel) == false) {
				head.add(rel);
//				System.out.println("a: " + a);
				printRuleSubTree(a, 0);
			}
		}
		System.out.println("====printRuleTree==== (END)");
	}
	
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("DatalogProgram\n");
		str.append("EDBs: " + EDBs + "\n");
		str.append("UDFs: " + UDFs + "\n");
		str.append("EDBToGenIdMap: " + EDBToGenIdMap + "\n");
		str.append("constructorForLB: " + constructorForLB + "\n");
		str.append("headRules: " + headRules + "\n");
		str.append("IndexSets: " + indexSets + "\n");
		str.append("Schema:\n");
		for (HashMap.Entry<String, Predicate> entry : predicates.entrySet()) {
			str.append("\t");
			str.append(entry.getValue());
			str.append("\n");
		}
			
		str.append("Rules:\n");
		for (HashMap.Entry<String, List<DatalogClause>> entry : rules.entrySet()) {
			for (DatalogClause c : entry.getValue()) {
				str.append("\t");
				str.append(c);
				str.append("\n");
			}
		}
		str.append("Rule head count: ").append(rules.size()).append("\n");
		str.append("Rule total count: ").append(ruleCount).append("\n");
		return str.toString();
	}

	public Set<String> getEDBs() {
		return EDBs;
	}

	public void setEDBs(Set<String> eDBs) {
		EDBs = eDBs;
	}

	public Set<String> getMaterializedIDBs() {
		return materializedIDBs;
	}

	public void setMaterializedIDBs(Set<String> materializedIDBs) {
		this.materializedIDBs = materializedIDBs;
	}

	public Set<String> getIDBs() {
		return IDBs;
	}

	public void setIDBs(Set<String> iDBs) {
		IDBs = iDBs;
	}
	
	public Set<String> getUDFs() {
		return UDFs;
	}
	
	public List<String> getHeadRules() {
		return headRules;
	}

	public int getCreatedViewId() {
		return createdViewId;
	}

	public void incCreatedViewId() {
		createdViewId++;
	}
	
	public void normalizeVarNames() {
		for (Map.Entry<String, List<DatalogClause>> entry : rules.entrySet()) {
			List<DatalogClause> cs = entry.getValue();
			for (DatalogClause c : cs) {
				c.normalizeVarNames();
			}
		}
	}

	public HashMap<String, HashMap<Integer, Integer>> getConstructorForLB() {
		return constructorForLB;
	}
	
	public void addConstructorForLB(String view, int rid, int size) {
		if (constructorForLB.containsKey(view) == false) {
			constructorForLB.put(view, new HashMap<Integer, Integer>());
		}
		constructorForLB.get(view).put(rid, size);
	}
	
}
