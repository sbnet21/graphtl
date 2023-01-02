package edu.upenn.cis.db.datalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.ConjunctiveQuery.Predicate;
import edu.upenn.cis.db.ConjunctiveQuery.Term;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.helper.Util;

public class DatalogQueryRewriter {
	final static Logger logger = LogManager.getLogger(DatalogQueryRewriter.class);
	private static int predIdx = 0;
	private static DatalogProgram program; /* given program */
	private static DatalogProgram rewrittenProgram;
	private static int varIdx = 0;
	
	/**
	 * Select atoms to be rewritten as a subquery. 
	 * It pushes down interpreted atoms to the subquery, if any.
	 */
	private static Set<Atom> getAtomsInSubquery(Atom pickedAtom, Set<Atom> atoms) {
		Set<Atom> atomsInSubquery = new LinkedHashSet<Atom>();
		Set<String> boundVars = new LinkedHashSet<String>();
		Set<String> pickedVars = getVars(pickedAtom);

		// 1. populate bound variables
		for (Atom a : atoms) {
			if (a.isInterpreted() == true && a.getTerms().get(1).isConstant() == true) {
				String v = a.getTerms().get(0).getVar();
				if (pickedVars.contains(v) == true) {
					boundVars.add(v);
				}
			} else if (program.getEDBs().contains(a.getRelName()) == true) {
				for (String v : getVars(a)) {
					if (pickedVars.contains(v) == true) {
						boundVars.add(v);
					}
				}
			}
		}
		
		// 2. populate interpreted atoms that contain any bound variables
		for (Atom a : atoms) {
			if (a.isNegated() == true) continue;
			for (Term t : a.getTerms()) {
				if (t.isVariable() == true && boundVars.contains(t.getVar()) == true) {
					atomsInSubquery.add(a);
					break;
				}
			}
		}

		// 3. populate vars in selected atoms
		Set<String> varsInSelectedAtoms = getVars(atomsInSubquery);
		for (Atom a : atoms) {
			if (a.isInterpreted() == false) continue;
			if (a.getPredicate().equals(Config.predOpEq) == true
				|| a.getPredicate().equals(Config.predOpGe) == true
				|| a.getPredicate().equals(Config.predOpGt) == true
				|| a.getPredicate().equals(Config.predOpLe) == true
				|| a.getPredicate().equals(Config.predOpLt) == true
			) {
				String v = a.getTerms().get(0).getVar();
				if (varsInSelectedAtoms.contains(v) == true) {
					atomsInSubquery.add(a);
				}
			}
		}
		atomsInSubquery.add(pickedAtom);
		
		return atomsInSubquery;
	}

	private static Atom getNewHeadForSubqueryOfBoundAtoms(Set<String> headVars, Atom a, 
			Set<Atom> relatedAtoms, Set<Atom> rwBody) {
		/**
		 * Among vars in relatedAtoms, if in atom, headVars, or other atoms  
		 */
		
		Atom headAtom = null;
		try {
			ArrayList<Term> terms = new ArrayList<Term>();
			headAtom = (Atom)a.clone();

			Set<String> vars1 = getVars(relatedAtoms);
			Set<String> vars2 = new LinkedHashSet<String>();
			Set<String> varsInNonRelatedAtoms = new LinkedHashSet<String>();
			
			vars2.addAll(headVars);
			vars2.addAll(getVars(a));
			
			for (Atom b : rwBody) {
				if (b.equals(a) == true) continue;
				if (relatedAtoms.contains(b) == true) {
					vars2.addAll(getVars(b));
				}
			}
			
			for (Atom b : rwBody) {
				if (relatedAtoms.contains(b) == false) {
					varsInNonRelatedAtoms.addAll(b.getVars());
				}
			}
			
			for (String var : vars1) { 
				if (vars2.contains(var) == true) {
					if (var.contentEquals("_") == false) {
						if (headVars.contains(var) == true || varsInNonRelatedAtoms.contains(var) == true) {
							terms.add(new Term(var, true));
						}
					}
				}
			}
			if (terms.size() == 0) {
				throw new IllegalArgumentException("terms.size == 0");
			}
			
			headAtom.setTerms(terms);
			headAtom.setNegated(false);
			headAtom.getPredicate().setRelName(getNewPred() + "_SUBQUERY_" + a.getPredicate().getRelName());
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return headAtom;
	}

	private static String getNewPred() {
		return "R_" + (predIdx++);
	}
	
	public static String getNewVar() {
		return "_v" + (varIdx++); 
	}

	/**
	 * Take a datalog program and a query, and return a rewritten query with additional (intermediate) rules.
	 */
	private static Set<Atom> getRewrittenBody(Set<String> headVars, Set<Atom> body, int depth) {
		Set<Atom> rwBody = new LinkedHashSet<Atom>(body);
//		System.out.println("[00-getRewrittenBody] rwBody: " + rwBody);
		handleAllPositiveIDBs(headVars, rwBody, depth);
		handleAllUDFs(headVars, rwBody, depth);
		handleAllNegativeIDBs(headVars, rwBody, depth);
	
		return rwBody;
	}

	private static ArrayList<Term> getTermsIncludeAllVars(Atom head, Set<Atom> atoms) {
		ArrayList<Term> terms = new ArrayList<Term>();

		Set<String> vars = new LinkedHashSet<String>();
		if (head != null) {
			for (Term t : head.getTerms()) {
				vars.add(t.toString());
			}
		}
		for (Atom a : atoms) {
			if (a.isNegated() == true) continue;
			for (Term t : a.getTerms()) {
				if (t.isVariable() == true) {
					vars.add(t.toString());
				}
			}
		}
		for (String v : vars) {
			if (v.contentEquals("_") == false) {
				terms.add(new Term(v, true));
			}
		}

		return terms;
	}
	
	private static ArrayList<Term> getTermsIncludeAllVars(Set<String> vars) {
		ArrayList<Term> terms = new ArrayList<Term>();
		for (String v : vars) {
			if (v.contentEquals("_") == false) {
				terms.add(new Term(v, true));
			}
		}
		if (terms.size() == 0) {
			throw new IllegalArgumentException("terms.size == 0");
		}
		return terms;
	}

	/**
	 * Return a set of variables in the atom.
	 * @param head
	 * @return
	 */
	private static Set<String> getVars(Atom head) {
		// TODO Auto-generated method stub
		Set<String> vars = new LinkedHashSet<String>();
		for (Term t : head.getTerms()) {
			if (t.isVariable() == true) {
				vars.add(t.toString());
			}
		}
		return vars;
	}

	private static Set<String> getVars(Set<Atom> atoms) {
		// TODO Auto-generated method stub
		Set<String> vars = new LinkedHashSet<String>();
		for (Atom a : atoms) {
			for (Term t : a.getTerms()) {
				if (t.isVariable() == true) {
					vars.add(t.toString());
				}
			}
		}
		return vars;
	}
	
	private static void handleAllUDFs(Set<String> headVars, Set<Atom> rwBody, int level) {
//		if (Config.isLogicBlox() == false) return;
		
		/*
		 	1. headVars <- E, U, Others
		 	2. Check if positive atoms (E) can determine UDF
		 	2. Create a rule: construct, newid, E' <- E (and insert)
		 	3. The head var of E' should be those in headVars and Others, and newid (replace E, U with E')
		 */
		
		HashSet<Atom> UDFs = selectUDFAtom(rwBody);
		HashSet<String> boundVars = new LinkedHashSet<String>();
		for (Atom b : rwBody) {
			if (program.getEDBs().contains(b) == true) {
				boundVars.addAll(b.getVars());
			}
		}
		
		for (Atom u : UDFs) {
//			System.out.println("[handleAllUDFs] uuuu: " + u);
			Set<Atom> posAtoms = new LinkedHashSet<Atom>();
			Set<String> varsInPosAtoms = new LinkedHashSet<String>();
			ArrayList<String> varsInUDFAtom = new ArrayList<String>();
			for (String v : u.getVars()) {
				varsInUDFAtom.add(v);
			}
			
//			System.out.println("\trwBody: " + rwBody + " program.getEDBs(): " + program.getEDBs());
			for (Atom a : rwBody) {
//				System.out.println("\t[######] u: " + u + " a: " + a + " a.isNegated: " + a.isNegated() + " a==u?" + (a==u));

				if (a == u) continue;
				if (a.isNegated() == true) continue;
				if (program.getEDBs().contains(a.getRelName()) == false && a.isInterpreted() == false) continue;
				
//				System.out.println("\t[######] passed u: " + u + " a: " + a);

				varsInPosAtoms.addAll(a.getVars());
				posAtoms.add(a);
			}
//			System.out.println("\tvarsInPosAtoms: " + varsInPosAtoms);
			
//			for (int i = 0; i < varsInUDFAtom.size()-1; i++) { // last element is the new id
//				String v = varsInUDFAtom.get(i);
//				if (varsInPosAtoms.contains(v) == false) {
//					throw new IllegalArgumentException("v: " + v + " is not in pos atoms varsInUDFAtom: " + varsInUDFAtom + " varsInPosAtoms: " + varsInPosAtoms + " rwBody: " + rwBody);
//				}
//			}
			
			String generatedId = u.getTerms().get(u.getTerms().size()-1).getVar();
			
//			System.out.println("\t********varsInPosAtoms: " + varsInPosAtoms + " generatedId: " + generatedId);
			
			if (varsInPosAtoms.contains(generatedId) == true) continue;
//			if (boundVars.contains(generatedId) == true) continue;
			
//			System.out.println("posAtoms: " + posAtoms + " uuu: " + u + " rwBody: " + rwBody + " varsInPosAtoms: " + varsInPosAtoms);
			
			DatalogClause c = new DatalogClause();
			Atom a1 = null;
			try {
				a1 = (Atom)u.clone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String constStr = u.getRelName();
			constStr = constStr.replace("_MAP_", "_CONST_");
			a1.getPredicate().setRelName(constStr);
			
			String newVar = varsInUDFAtom.get(varsInUDFAtom.size()-1) + "_newobj";
			a1.getTerms().get(a1.getTerms().size()-1).setVar(newVar);
			
			
			Atom a2 = new Atom(Config.relname_gennewid + "_v0");
			a2.appendTerm(new Term(newVar, true));

//			Atom a3 = new Atom(getNewPred() + "_HANDLE_" + u.getRelName());
			Atom a3 = new Atom(getNewPred() + "_" + u.getRelName() + "_HANDLE");
			for (String v : varsInPosAtoms) {
				a3.appendTerm(new Term(v, true));
			}

			c.addAtomToHeads(a1);
			c.addAtomToHeads(a2);
//			c.addAtomToHeads(a3);
			for (Atom a : posAtoms) {
				if (a != u) {
					c.addAtomToBody(a);
				}
			}
			
			boolean willAddToProgram = true;
			for (Atom a : c.getBody()) {
//				if (a.getRelName().contentEquals("R_24_SUBQUERY_MAP_v0") == true) {
//					System.out.println("TTTTTT: " + rewrittenProgram.getEDBToGenIdMap() + " c: " + c + " c.getHeads().get(0).getVars(): " + c.getHeads().get(0).getVars());
//					System.out.println("TTTTTT a: " + a);
//				}
				
				if (rewrittenProgram.getEDBToGenIdMap().containsKey(a.getRelName()) == true) {
					willAddToProgram = false;

//					HashSet<Integer> hs = rewrittenProgram.getEDBToGenIdMap().get(a.getRelName());
//					System.out.println("TTTTTT hs: " + hs);
//					for (int i = 0; i < c.getHeads().get(0).getTerms().size(); i++) {
//						if (hs.contains(i) == true) {
//							String var = a.getTerms().get(i).getVar();
//							System.out.println("TTTT3: " + var + " i: " + i);
//							if (c.getHeads().get(0).getVars().contains(var) == true) {
//								willAddToProgram = false;
//								break;
//							}
//						}
//					}
				}
//				if (a.getRelName().contentEquals("R_24_SUBQUERY_MAP_v0") == true) {
//					System.out.println("TTTTTT2: willAddToProgram: " + willAddToProgram );
//				}
//				
			}

//			System.out.println("UDFHandled (before) ccrwBody: " + rwBody + " willAddToProgram: " + willAddToProgram + " posAtoms: " + posAtoms);

			if (willAddToProgram == true) {
				rewrittenProgram.addRule(c);
//				rwBody.removeAll(posAtoms);
//				rwBody.add(a3);
			}

			

//			rwBody.remove(u);
			
//			System.out.println("UDFHandled ccrwBody: " + rwBody + " headVars: " + headVars);
		}
	}
	
	private static HashSet<Atom> selectUDFAtom(Set<Atom> rwBody) {
		HashSet<Atom> selected = new HashSet<Atom>();
		for (Atom a : rwBody) {
			if (a.getRelName().startsWith(Config.relname_gennewid + "_") == true) {
				selected.add(a);
			}
		}		
		return selected;
	}
	
	
	
	private static void handleAllNegativeIDBs(Set<String> headVars, Set<Atom> rwBody, int level) {
		/**
		 * A(a,b),!B(a,b),C(c,d),a=1 // given a body of atoms. 
		 * 		When we handle !B(a,b), A(a,b),a=1 are called determined atoms that constraint (a part of) B(a,b).
		 * Related R'(a,b)<-A(a,b),a=1 // generate a subquery from a set of determined atoms,
		 * Related B'(a,b)<-R'(a,b),B(a,b) // creating a subquery from constraint atom of the selected negated atom
		 * Rewritten to R'(a,b),!B'(a,b),C(c,d) // rewritten atoms
		 */
//		Util.console_logln("[START-handleAllNegativeIDBs] headVars: " + headVars + " rwBody: " + rwBody, 4);
		while(true) {
			Atom a = selectNegativeIDBAtom(rwBody);
			if (a == null) {
				break;
			}
//			Util.console_logln("[handleAllNegativeIDBs] selectNegativeIDBAtom: " + a, 4);

			// Select a set of related atoms of the negated atom and create a sub query.
			Set<Atom> relatedAtoms = selectRelatedAtoms(headVars, a, rwBody);
			Set<Atom> nonRelatedAtoms = new LinkedHashSet<Atom>(rwBody);
			nonRelatedAtoms.removeAll(relatedAtoms);
			nonRelatedAtoms.remove(a);
			
			Atom headAtom = getNewHeadForSubqueryOfBoundAtoms(headVars, a, relatedAtoms, rwBody); // A'
//			Util.console_logln("[handleAllNegativeIDBs] getNewHeadForSubqueryOfBoundAtoms: " + headAtom, 4);

			Set<String> candidateHeadVars = new LinkedHashSet<String>();
			Set<String> varsInRelatedAtoms = getVars(relatedAtoms);
			Set<String> varsInNonRelatedAtoms = getVars(nonRelatedAtoms);
			
			for (String v : headVars) {
				if (varsInRelatedAtoms.contains(v) == true) {
					candidateHeadVars.add(v);
				}
			}
			for (String v : varsInRelatedAtoms) {
				if (varsInNonRelatedAtoms.contains(v) == true) {
					candidateHeadVars.add(v);
				}
			}
			headAtom.setTerms(getTermsIncludeAllVars(candidateHeadVars));
			
//			Util.console_logln("[handleAllNegativeIDBs] (updated) getNewHeadForSubqueryOfBoundAtoms headAtom: " + headAtom, 4);
//			Util.console_logln("[handleAllNegativeIDBs] (updated) getNewHeadForSubqueryOfBoundAtoms relatedAtoms: " + relatedAtoms, 4);

			program.getEDBs().add(headAtom.getPredicate().getRelName());
			rewrittenProgram.addRule(new DatalogClause(headAtom, relatedAtoms));

			boolean ret = false; 
			if (Config.isSubQueryPruningEnabled() == true) {
				rewrittenProgram.runRule(headAtom.getPredicate().getRelName());
				ret = rewrittenProgram.checkZeroPred(headAtom.getPredicate().getRelName());
			}
			if (ret == true) { // positive atoms has 0-size
				rwBody.clear();
				return;
			}			
			rwBody.remove(a);
			rwBody.removeAll(relatedAtoms);

			// subquery with positive atom
			Atom posHeadAtom = null; 
			try {
				posHeadAtom = (Atom)a.clone();
				posHeadAtom.setNegated(false);
				posHeadAtom.getPredicate().setRelName(getNewPred() + "_HANDLE_NEG_" + a.getPredicate().getRelName());
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			Util.console_logln("[handleAllNegativeIDBs] posHeadAtom: " + posHeadAtom, 4);

			Atom negatedAtomWithoutTheNegation = null; // B (of !B)
			try {
				negatedAtomWithoutTheNegation = (Atom)a.clone();
				negatedAtomWithoutTheNegation.setNegated(false);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			Util.console_logln("[handleAllNegativeIDBs] negatedAtomWithoutTheNegation: " + negatedAtomWithoutTheNegation, 4);

			Set<Atom> posAtoms = new LinkedHashSet<Atom>();
			posAtoms.add(negatedAtomWithoutTheNegation);
			posAtoms.add(headAtom);
	
			Set<Atom> unfolding = getRewrittenBody(getVars(a), posAtoms, level+1);
			if (unfolding.isEmpty() == true) { // B has nothing, so !B can be pruned.
				rwBody.add(headAtom);
				continue;
			}
			posHeadAtom.setTerms(getTermsIncludeAllVars(null, unfolding));
//			System.out.println("posHeadAtom: " + posHeadAtom);
			
			program.getEDBs().add(posHeadAtom.getPredicate().getRelName());
			rewrittenProgram.addRule(new DatalogClause(posHeadAtom, unfolding));
			
			ret = false;
			if (Config.isSubQueryPruningEnabled() == true) {
				rewrittenProgram.runRule(posHeadAtom.getPredicate().getRelName());
				ret = rewrittenProgram.checkZeroPred(posHeadAtom.getPredicate().getRelName());
			}

			// rewrite original query with negation
			Atom negHeadAtom = null; // B'
			try {
				negHeadAtom = (Atom)posHeadAtom.clone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			negHeadAtom.getPredicate().setRelName(getNewPred() + "_" + a.getPredicate().getRelName());

			Set<Atom> negAtoms = new LinkedHashSet<Atom>();
			negAtoms.add(headAtom); // !B'
			Atom negPosHeadAtom = null;
			try {
				negPosHeadAtom = (Atom)posHeadAtom.clone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			negPosHeadAtom.setNegated(true);
			negAtoms.add(negPosHeadAtom); // A'

			program.getEDBs().add(negHeadAtom.getPredicate().getRelName());
			negHeadAtom.setTerms(getTermsIncludeAllVars(candidateHeadVars));
			
			rewrittenProgram.addRule(new DatalogClause(negHeadAtom, negAtoms));
			
			if (Config.isSubQueryPruningEnabled() == true) {
				rewrittenProgram.runRule(negHeadAtom.getPredicate().getRelName());
				ret = rewrittenProgram.checkZeroPred(negHeadAtom.getPredicate().getRelName());
			}
			rwBody.add(negHeadAtom);
		}
//		Util.console_logln("[END-handleAllNegativeIDBs] headVars: " + headVars + " rwBody: " + rwBody, 4);
	}

	private static void unfoldSingleQuery(Atom pickedAtom, Set<Atom> rwBody, DatalogClause rule) {
		Set<Atom> unfolding = unfoldAtom(pickedAtom, rule);
		rwBody.addAll(unfolding);
		rwBody.remove(pickedAtom);
	}

	private static void unfoldDisjunctiveQuery(Set<String> headVars, Set<Atom> rwBody, Atom pickedAtom, List<DatalogClause> rules, int depth) {
		// 1. Set up the head with a new name
		Atom unfoldHeadAtom = null;
		try {
			unfoldHeadAtom = (Atom)pickedAtom.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		unfoldHeadAtom.setPredicate(new Predicate(getNewPred() + "_" + pickedAtom.getRelName()));

		// 1. Set up the head
		Set<Atom> atomsInSubquery = getAtomsInSubquery(pickedAtom, rwBody);
		
		Set<Atom> nonRelatedAtoms = new LinkedHashSet<Atom>();
		nonRelatedAtoms.addAll(rwBody);
		nonRelatedAtoms.removeAll(atomsInSubquery);
						
		Set<String> candidateHeadVars = new LinkedHashSet<String>(); //getVars(pickedAtom);
		Set<String> varsInRelatedAtoms = getVars(atomsInSubquery);
		Set<String> varsInNonRelatedAtoms = getVars(nonRelatedAtoms);

		for (String v : headVars) {
			if (varsInRelatedAtoms.contains(v) == true) {
				candidateHeadVars.add(v);
			}
		}
		for (String v : varsInRelatedAtoms) {
			if (varsInNonRelatedAtoms.contains(v) == true) {
				candidateHeadVars.add(v);
			}
		}

		ArrayList<Term> terms = getTermsIncludeAllVars(candidateHeadVars);
		unfoldHeadAtom.setTerms(terms);

		int numOfEmpty = 0;
		for (int i = 0; i < rules.size(); i++) {
//			System.out.println("rule to apply: " + rules.get(i));
			DatalogClause r = new DatalogClause();
			ArrayList<Atom> ar = rules.get(i).getHead().getAtomBodyStrWithInterpretedAtoms("");
			r.setHead(ar.get(0));
			for (Atom a : rules.get(i).getBody()) {
				r.addAtomToBody(a);
			}
			for (int j = 1; j < ar.size(); j++) {
				r.addAtomToBody(ar.get(j));
			}
//			System.out.println("rule to apply (revised): " + r);

			Set<Atom> unfolding = unfoldAtom(pickedAtom, r);
//			System.out.println("unfolding: " + unfolding);

			if (unfolding.isEmpty() == true) {
				numOfEmpty++;
				throw new IllegalArgumentException("Unfolding is empty");
			}
			unfolding.addAll(atomsInSubquery);
			unfolding.remove(pickedAtom);
//			System.out.println("[unfoldDisjunctiveQuery] rulesToBeUnioned qqq2: " + rules);
			Set<Atom> reUnfolding = getRewrittenBody(getVars(unfoldHeadAtom), unfolding, depth+1);
			if (reUnfolding.isEmpty() == true) {
				numOfEmpty++;
				continue;
			}						
			program.getEDBs().add(unfoldHeadAtom.getPredicate().getRelName());
			rewrittenProgram.addRule(new DatalogClause(unfoldHeadAtom, reUnfolding));
		}
		
		boolean ret = false;
		if (Config.isSubQueryPruningEnabled() == true && numOfEmpty != rules.size()) {
			rewrittenProgram.runRule(unfoldHeadAtom.getPredicate().getRelName());
			ret = rewrittenProgram.checkZeroPred(unfoldHeadAtom.getPredicate().getRelName());
		}
		if (numOfEmpty == rules.size()) {
			ret = true;
		}
		if (rwBody.isEmpty() == true) {
			return;
		}
		if (ret == true) {
			rwBody.clear();
			return;
		} else {
			rwBody.add(unfoldHeadAtom);
			rwBody.removeAll(atomsInSubquery);
		}		
	}

	private static void handleAllPositiveIDBs(Set<String> headVars, Set<Atom> rwBody, int depth) {
		while(true) {
			Atom pickedAtom = selectPositiveIDBAtom(rwBody);
			if (pickedAtom == null) { // all atoms are unfolded
				break;
			}
//			System.out.println("[01-handleAllPositiveIDBs] pickedAtom: " + pickedAtom + " isNeg: " + pickedAtom.isNegated() + " from rwBody: " + rwBody);
			
			List<DatalogClause> rules = program.getRules(pickedAtom.getRelName());
			if (rules == null) {
				throw new IllegalArgumentException("[ERROR] No rule found for pred: " + pickedAtom.getRelName());
			}

			if (rules.size() == 1) { // not a disjunctive query
				unfoldSingleQuery(pickedAtom, rwBody, rules.get(0)); 
			} else { // create a disjunctive query
				unfoldDisjunctiveQuery(headVars, rwBody, pickedAtom, rules, depth);
			}
		}
		
		// Validity check
//		for (Atom a : rwBody) {
//			if (a.isInterpreted() == false && a.isNegated() == false && program.getUDFs().contains(a.getRelName()) == false 
//					&& program.getEDBs().contains(a.getPredicate().getRelName()) == false) {
//				System.out.println("program: " + program);
//				throw new IllegalArgumentException("[ERROR] Violate assertion. can be unfolded - atom: " + a + " rwBody: " + rwBody);				
//			}
//		}
	}

	/**
	 * Select a negated IDB atom to process. 
	 * A candidate atom is a negated atom that is neither materialized nor interpreted atom.
	 * This method is called only when there is no positive atom that can be unfolded. 
	 * There exists at least one variable that appears in a positive atom.   
	 */
	private static Atom selectNegativeIDBAtom(Set<Atom> atoms) {
		Atom selectedAtom = null;

		for (Atom a : atoms) {
			if (a.isNegated() == false) continue;
			if (a.isInterpreted() == true) continue;
			if (program.getEDBs().contains(a.getRelName()) == true) continue;
			if (program.getUDFs().contains(a.getRelName()) == true) continue;
			selectedAtom = a;
			break;
		}

		//FIXME: check
		if (selectedAtom != null) {
			Set<String> vars = getVars(selectedAtom);
			boolean isValid = false;
			if (selectedAtom != null) {
				for (Atom a : atoms) {
					if (a.isNegated() == true) continue;
					for (Term t : a.getTerms()) {
						if (t.isVariable() == true) {
							if (vars.contains(t.toString()) == true) {
								isValid = true;
								break;
							}
						}
					}
					if (isValid == true) break;
				}
				if (isValid == false) {
					throw new IllegalArgumentException("[ERROR] selectNegativeIDBAtom incorrectly.");				
				}
			}
		}

		return selectedAtom;
	}

	private static Atom selectPositiveIDBAtom(Set<Atom> atoms) {
		Atom selectedAtom = null;

		for (Atom a : atoms) {
			if (a.isNegated() == true) continue;
			if (program.getEDBs().contains(a.getPredicate().getRelName()) == true) continue;
			if (program.getUDFs().contains(a.getPredicate().getRelName()) == true) continue;
			if (a.isInterpreted() == true) continue;
			if (program.getRules(a.getPredicate().getRelName()) == null) continue;
			selectedAtom = a;
			break;
		}

		if (selectedAtom == null) {
			for (Atom a : atoms) {
				if (a.isNegated() == true) continue;
				if (program.getEDBs().contains(a.getPredicate().getRelName()) == true) continue;
				if (program.getUDFs().contains(a.getPredicate().getRelName()) == true) continue;
				if (a.isInterpreted() == true) continue;
				if (program.getRules(a.getPredicate().getRelName()) == null) continue;
				selectedAtom = a;
			}
		}
		logger.debug("[selectPositiveIDBAtom] selectedAtom: " + selectedAtom);
//
//		if (selectedAtom != null && selectedAtom.getPredicate().getRelName().equals("N_delta_v1v") == true) {
//			System.out.println("selectPosIDBatom..: " + selectedAtom.getPredicate().toString());
//		}
		return selectedAtom;
	}

	/**
	 * Return a set of related atoms.
	 */
	private static Set<Atom> selectRelatedAtoms(Set<String> headVars, Atom a, Set<Atom> rwBody) {
		Set<Atom> atoms = new LinkedHashSet<Atom>();
		Set<String> boundVars = getVars(a);
		boundVars.addAll(headVars);
		
//		System.out.println("[START-selectRelatedAtoms] headVars: " + headVars + " a: " + a + " rwBody: " + rwBody +  " boundVars: "+ boundVars);
		// found all (transitively) associated atoms from negated atom and headVars 
		int numOfAtoms = 0;
		while(true) {
			for (Atom ar : rwBody) {
				if (ar.isNegated() == true) continue;
				for (Term t : ar.getTerms()) {
					if (t.isVariable() == false) continue;
					if (boundVars.contains(t.toString()) == true) {
						boundVars.addAll(ar.getVars());
						atoms.add(ar);
						break;
					}
				}
			}
			// check if we found all atoms to include
			if (numOfAtoms == atoms.size()) {
				break;
			}
			numOfAtoms = atoms.size();
		}
		if (atoms.size() == 0) {
			throw new IllegalArgumentException("ERROR... EMPTY RELATED ATOMS atoms: " + atoms);
		}
//		System.out.println("[END-selectRelatedAtoms] headVars: " + headVars + " a: " + a + " rwBody: " + rwBody + " atoms: " + atoms);
		return atoms;
	}
	
	/**
	 * An atom of a rule is unfolded by its rule and 
	 * the unfolding is inserted to the original rule 
	 *  
	 * @param a An atom in a rule _c_, to unfold 
	 * @param body A rule to unfold
	 * @return Unfolding
	 */
	private static Set<Atom> unfoldAtom(Atom a, DatalogClause rule) {
		/*
		 1. bound vars (in a)
		 2. unbound vars should be replaced unique vars
		 */
		HashSet<String> boundVars = a.getVars();
		HashSet<String> varsInBody = new HashSet<String>();
		HashMap<String, String> varMap = new HashMap<String, String>();

		Atom h = rule.getHead();
		for (int i = 0; i < h.getTerms().size(); i++) {
			Term t = h.getTerms().get(i);

//			if (t.isVariable() == false || t.getVar().equals("_")) {
//				throw new IllegalArgumentException("[unfoldAtom] WRONG... term t: " + t + " in head: " + h + " for Atom a: " + a + " and rule: " + rule);
//			}
			String var = a.getTerms().get(i).getVar();
			if (var.equals("_") == false) {
				varMap.put(t.getVar(), var);
			}
		}
		for (Atom b : rule.getBody()) {
			varsInBody.addAll(b.getVars());
		}
		for (String var : varsInBody) {
			if (var.equals("_") == true) continue;
			if (varMap.containsKey(var) == false) {
				varMap.put(var, getNewVar());
			}
		}
//		System.out.println("[unfoldAtom] boundVars: " + boundVars + " a: " + a);
//		System.out.println("[unfoldAtom] varsInBody: " + varsInBody);
		
		for (String var : varsInBody) {
			if (boundVars.contains(var) == false) {
				if (varMap.containsKey(var) == false) {
					String newVar = getNewVar();
//					System.out.println("varToNewVar: " + var + "->" + newVar);
					varMap.put(var, newVar);
				}
			}
		}
//		System.out.println("varMap: " + varMap);
		Set<Atom> unfolding = new LinkedHashSet<Atom>();
		for (int i = 0; i < rule.getBody().size(); i++) {
			Atom b = null;
			try {
				b = (Atom)(rule.getBody().get(i)).clone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (Term t : b.getTerms()) {
				if (t.isVariable() == false) continue;
				if (t.getVar().equals("_")) continue;
				if (varMap.containsKey(t.getVar()) == true) {
					t.setVar(varMap.get(t.getVar()));
				}
			}
			unfolding.add(b);
		}
		return unfolding;
	}

	
	/**
	 * Get a datalog program of the rewritten queries (entry point)
	 */
	public static DatalogProgram getProgramForRewrittenQuery(DatalogProgram p, DatalogClause q) {
		if (q.getHeads().size() > 1) {
			throw new IllegalArgumentException("Query should have 1 head.");
		}
		program = p;
		rewrittenProgram = new DatalogProgram();

		Set<String> headVars = new LinkedHashSet<String>();
		for (Atom h : q.getHeads()) { //FIXME: multiple heads?
			headVars.addAll(getVars(h));
		}
		Set<Atom> body = new LinkedHashSet<Atom>();
		body.addAll(q.getBody());

		Set<Atom> rwBody = getRewrittenBody(headVars, body, 0);
		
//		System.out.println("[getProgramForRewrittenQuery] headVars: " + headVars + " rwBody: " + rwBody + " q: " + q);
		
		if (rwBody.isEmpty() == false) {
			Atom newHead = new Atom(new Predicate(Config.relname_query));
			for (String v : headVars) {
				newHead.appendTerm(new Term(v, true));
			}
			rewrittenProgram.addRule(new DatalogClause(newHead, rwBody));			
//			System.out.println("rewrittenProgram2: " + rewrittenProgram);

//			if (Config.isAnswerEnabled() == true) {
//				//FIXME: how about q.getHead()?
//				for (Atom h : q.getHeads()) {
//					DatalogClause c = new DatalogClause();
//					c.setHead(h);
//					c.addAtomToBody(newHead);
//					rewrittenProgram.addRule(c);
//				}
//				//FIXME: need to check checkZeroPred on this?
//			}
		}
		return rewrittenProgram;
	}	
}
