package test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.datalog.DatalogClause;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.graphtrans.datastructure.Egd;
import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
import edu.upenn.cis.db.graphtrans.parser.EgdParser;
import edu.upenn.cis.db.graphtrans.parser.QueryParser;
import edu.upenn.cis.db.graphtrans.parser.TransRuleParser;

public class QueryContainment {
	private static ArrayList<String> nv;
	private static ArrayList<String> ev;

	private static ArrayList<String> unfolding;
	
	private static ArrayList<ArrayList<Integer>> parts;
	
	
	private static void unfold(String cq, int index) {
		String atom = cq.substring(index, index+1);
		
		System.out.println("[unfold] index: " + index + " atom: " + atom);
		ArrayList<String> v;
		if (atom.equals("N") == true) {
			v = nv;
		} else {
			v = ev;
		}
		for (int i = 0; i < v.size(); i++) {
			unfolding.set(index, v.get(i));
			if (index + 1 < cq.length()) {
				unfold(cq, index+1);
			} else {
				System.out.println(unfolding);
				// for q, partition p of var(Q) and create canonical DBs (q,p,...)
			}
		}		
	}
	
	/*
	 true if q1 is contained in r's SSR under egds
	 For a given transformation rule r, compute N', E', each as unions of CQ while disregarding negated atoms
			N'(n,l) := N(n,l),!N-(n,l) U N+(n,l),!N-(n,l)
			E'(e,f,t,l) := E(e,f,t,l,),!E-(e,f,t,l) U E+(e,f,t,l),!E-(e,f,t,l)
			Output 
				N'(n,l) := ... U ... U ... U ... (no negation)
				E'(e,f,t,l) := ... U ... U ... U ... (no negation)
	 Unfold q1 using N', E' as unions of CQ
	 Create each canonical database using set partition approach (as in Ullmann's paper)
	 For each database, chase with egds (in any order, until no change)
	 If there exists q1(D) that is not contained in q2(D), then false. Otherwise, true.	 
	 */
	public static boolean isContained(TransRule r, DatalogClause q, ArrayList<Egd> egds) {
	 	// For a rule r w/ collapsing only
	 	
		// qssr is r's output pattern's unfolding
		DatalogClause qssr = new DatalogClause();
		if (r != null) {
			qssr.setBody(r.getPatternAfter());
		}
		if (q != null) {
			qssr.setHead(q.getHead());
		}
		System.out.println("qssr: " + qssr);
		
	 	// Create N1' and E1' from r, while disregarding negated atoms
		ArrayList<String> unfoldingsOfNV = new ArrayList<String>();
		ArrayList<String> unfoldingsOfEV = new ArrayList<String>();
		populateUnfoldingWithoutNegation(r, unfoldingsOfNV, unfoldingsOfEV);
		
	 	// 2. create Q' as unions of CQs with N1' and E1' above.
		ArrayList<ArrayList<Atom>> UCQofQ; 
		UCQofQ = populateUCQofQuery(q, unfoldingsOfNV, unfoldingsOfEV); //	Q' is a list of CQs
	 	
		for (int i = 0; i < UCQofQ.size(); i++) {
			ArrayList<Atom> cq = UCQofQ.get(i);
			
			int numOfVars = 3; // TODO: get vars from CQ
			// TODO: enumerate all partitions and construct CDB
			String cdb = "FDF";
			
			Set<String> baseSet = new HashSet<String>();
	        baseSet.add("a");
	        baseSet.add("b");
	        baseSet.add("c");
			PartitionSetCreator<String> partSetCreatorEmpty = new PartitionSetCreator<String>(baseSet);
			Set<Set<Set<String>>> partitionSetsEmpty = partSetCreatorEmpty.findAllPartitions();
			
			System.out.println("Result:  " + partitionSetsEmpty);
	
			chaseCDB(cdb, egds); // TODO: update Datalog (or delete if inconsistent)
			boolean isContainedIn = isContainedIn(cq, qssr, cdb);
			if (isContainedIn == false) {
				return false;
			}
		}
		return true;
	}

	private static boolean isContainedIn(ArrayList<Atom> cq, DatalogClause qssr, String cdb) {
		// TODO Auto-generated method stub
		return false;
	}

	private static boolean chaseCDB(String cdb, ArrayList<Egd> egds) {
		// TODO Auto-generated method stub
		/*
	 		while true
	 			noDBupdate = true // no update then chase is done
 				for each s:P->Q in S
 					while true : LABEL_A
 						R = select r from D where P
 						isAllChecked = true
		 				for each r in
	 						if check(Q) = false
	 							update D accordinly 
	 							isAllChecked = false
	 							noDBupdate = false
	 					if isAllChecked = true
	 						break
	 			if noDBupdate = true
	 				break
 			return D chased database
		 */
		return false;
	}

	private static ArrayList<ArrayList<Atom>> populateUCQofQuery(DatalogClause q, ArrayList<String> unfoldingsOfN, ArrayList<String> unfoldingsOfE) {
		// TODO Auto-generated method stub
		/*
 	 		P = get partitions
	 		for each partition p in P
	 			create DB by mapping
	 		return CDBs
		 */
		ArrayList<ArrayList<Atom>> UCQofQ = new ArrayList<ArrayList<Atom>>();
		
		return UCQofQ;
	}

	private static void populateUnfoldingWithoutNegation(TransRule r, ArrayList<String> unfoldingsOfN, ArrayList<String> unfoldingsOfE) {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("[Start] QueryContainment");
		
		nv = new ArrayList<String>();
		ev = new ArrayList<String>();
		
		nv.add("N1");
		nv.add("N2");
		nv.add("N3");
		
		ev.add("E1");
		ev.add("E2");
		ev.add("E3");
		ev.add("E4");

		String query = "NNE";
		unfolding = new ArrayList<String>();
		for (int i = 0; i < query.length(); i++) {
			unfolding.add(query.substring(i, i+1));
		}
		
		unfold(query, 0);
		
//		int n = 4;
//		parts = new ArrayList<ArrayList<Integer>>();
//		partition(n);

		Set<Integer> baseSet = new HashSet<Integer>();
        baseSet.add(1);
        baseSet.add(2);
        baseSet.add(3);
        baseSet.add(4);
		PartitionSetCreator<Integer> partSetCreatorEmpty = new PartitionSetCreator<Integer>(baseSet);
		Set<Set<Set<Integer>>> partitionSetsEmpty = partSetCreatorEmpty.findAllPartitions();
		System.out.println("BaseSet: " + baseSet);
		System.out.println("Result:  " + partitionSetsEmpty);
		System.out.println("Base-Size: " + baseSet.size() + " Result-Size: " + partitionSetsEmpty.size());

		Config.initialize();

		String rule = "match a:A-e1:X->b:B, b-e2:Y->c:C map (b,c) to s:S";
		TransRuleParser parser = new TransRuleParser();
		TransRule r = parser.Parse(rule);
		r.computePatterns();
//		r.show();

		String userquery = "match a:A-e1:X->s:S, s-e2:Y->s return a,s";
		QueryParser qparser = new QueryParser();
		DatalogClause q = qparser.Parse(userquery);
		
		System.out.println("q: " + q);
		
		ArrayList<Egd> egds = new ArrayList<Egd>();
		String egd = "N(n1,l1),N(n2,l2),N(n3,l3),E(e1,n1,n3,l4),E(e2,n2,n3,l5) -> e1=e2";
		Egd e = EgdParser.Parse(egd);
		egds.add(e);
		
		System.out.println("egds: " + egds);
		isContained(r, q, egds);
		
		System.out.println("[End] QueryContainment");
	}

}


