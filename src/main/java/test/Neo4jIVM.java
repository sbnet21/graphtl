package test;

import java.util.HashSet;
import java.util.LinkedHashSet;

public class Neo4jIVM {
	private static int numOfViews = 3; // G,V1,V2,V3 if 3
	private static HashSet<Integer> finalizedViews = new LinkedHashSet<Integer>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("[Start] Neo4jIVM");

//		System.out.println("Initialize D["+ i + "]=empty");

		System.out.println("Input V[0]=G, D[0]=(D+[0], D-[0]) such that D+[0]=G+, D-[0]=G-");
		String prefix = "";
		for (int i = 1; i <= numOfViews; i++) {
//			System.out.println("Handle from V["+ i + "]");			
			for (int j = i; j <= numOfViews; j++) {
				prefix = "i: " + i + " j: " + j + "=> ";

//				System.out.println(prefix + "Handle V" + j);
				System.out.println(prefix + "[COMPUTE_DELTA] With D-[" + (j-1) + "], find matches M of V[" + (j-1) + "] by R[" + j + "], compute Rollback D[" + j + "]");
				
				if (j == numOfViews) {
					System.out.println(prefix + "[UPDATE_VIEW] At the top stratum apply D[" + j + "] to V[" + j + "]");
					System.out.println(prefix + "[UPDATE_VIEW_FINAL] At the bottom stratum (among not applied) apply D[" + (i-1) + "] to V[" + (i-1) + "]");
					finalizedViews.add(i-1);
					System.out.println(prefix + "\t[INFO] finalizedViews[" + finalizedViews + "]");
					System.out.println(prefix + "[COMPUTE_DLETA], At the bottom stratum (among not applied) with D+[" + (i-1) + "] to V[" + (i-1) + "] and compute D[" + i + "]");
					
					if (i == numOfViews) {
						System.out.println(prefix + "[UPDATE_VIEW_FINAL] At the bottom stratum (among not applied) apply D[" + i + "] to V[" + i + "] (now finalized)");
						finalizedViews.add(i);
						System.out.println(prefix + "\t[INFO] finalizedViews[" + finalizedViews + "]");
					}
				}				
			}
		}
		
		System.out.println("[End] Neo4jIVM");
	}

}


/*
	D-, D+ interact?
		
		map, add, remove
		map s1,s2 to t
			del t
			del edges to t
			addback s1,s1
			addback edges to s1,s2
		add node -> del node
		add edge -> del edge
		del node -> add node
		del edge -> add edge
	
	
	if they are from different rules.

	G(level=0) and G'(level=1)
	
	for each insertion of node N(n,l), just add it to G' with c=1,d=99
	
	for inserted edge E(e,f,t,l)
		INSERT it to G' with c=1,d=99		 
		for each rule r 
			find MATCH where one of edge id is e
			if found
				keep it in MAP(s,t,T) in java, according to the rule
	
		if MAP is not empty
			INSERT new node for each t in MAP(s,t,T) (c=1,d=99) and keep tack of the ids.
			DELETE node s (by d=1)
			
			for each s in MAP(s,t,T)
				find MATCH E(e,s,t,l) with d>0
					DELETE the edge d=1
					INSERT new edge with replaced by s
				find MATCH E(e,f,s,l) with d>0
					DELETE the edge d=1
					INSERT new edge with replaced by s
		truncate MAP
*/









