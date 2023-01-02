package edu.upenn.cis.db.graphtrans.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.db.ConjunctiveQuery.Atom;
import edu.upenn.cis.db.ConjunctiveQuery.Term;
import edu.upenn.cis.db.graphtrans.Config;
import edu.upenn.cis.db.graphtrans.GraphQueryParser.GraphTransQueryBaseVisitor;
import edu.upenn.cis.db.graphtrans.GraphQueryParser.GraphTransQueryLexer;
import edu.upenn.cis.db.graphtrans.GraphQueryParser.GraphTransQueryParser;
import edu.upenn.cis.db.graphtrans.GraphQueryParser.GraphTransQueryParser.HopContext;
import edu.upenn.cis.db.graphtrans.GraphQueryParser.GraphTransQueryParser.Term_bodyContext;
import edu.upenn.cis.db.graphtrans.datastructure.TransRule;
import edu.upenn.cis.db.helper.Util;

public class TransRuleParser extends GraphTransQueryBaseVisitor<Void> {
	final static Logger logger = LogManager.getLogger(TransRuleParser.class);

	private TransRule transRule;
	private String query;

	private HashMap<String, HashMap<String, ArrayList<Atom>>> propertyAtoms; // var |-> (prop |-> predicates)


	public TransRuleParser() {
	}

	public TransRule Parse(String query) {
		this.query = query;
		
		propertyAtoms = new HashMap<String, HashMap<String, ArrayList<Atom>>>();
		transRule = new TransRule(query);

		logger.trace("TransRuleParser query: " + query);

		CharStream input = CharStreams.fromString(query);
		GraphTransQueryLexer lexer = new GraphTransQueryLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		GraphTransQueryParser parser = new GraphTransQueryParser(tokens);
		ParseTree tree = parser.trans_rule();
		visit(tree);

		//logger.trace("result: " + result);
		//logger.trace(tree.toStringTree(parser)); // print LISP-style tree
		//logger.trace("transRule: " + trans_rule);

		//transRule.show();

		return transRule;
	}

	@Override 
	public Void visitTrans_rule(GraphTransQueryParser.Trans_ruleContext ctx) { 
		int start = ctx.getStart().getStartIndex(); 
		int stop = ctx.getStop().getStopIndex() + 1;
		String subquery = query.substring(start, stop);
		logger.trace("transRule: " + subquery);

		return visitChildren(ctx); 
	}
	
	@Override public Void visitWhere_clause(GraphTransQueryParser.Where_clauseContext ctx) {
		visitChildren(ctx);
		ParserHelper.processWhereClause(transRule.getNodeVarToLabelMap(), transRule.getPatternMatch(), propertyAtoms, transRule.getVarsInWhereClause());

		System.out.println(Util.ANSI_GREEN + "propertyAtoms: " + propertyAtoms + Util.ANSI_RESET);
		
		return null;
	}

	@Override public Void visitWhere_condition(GraphTransQueryParser.Where_conditionContext ctx) {
		String var = ctx.lop().var().getText();
		String prop = "";
		if (ctx.lop().prop() != null) {
			prop = ctx.lop().prop().getText();
		}
		String op = ctx.operator().getText();
		String val = ctx.rop().propValue().int_or_literal().getText();		
		
		ParserHelper.processWhereCondition(var, prop, op, val, transRule.getPatternMatch(), propertyAtoms, transRule.getVarsInWhereClause(), transRule.getWhereConditionForNeo4j());

		return visitChildren(ctx); 
	}
	
	@Override 
	public Void visitMatch_clause(GraphTransQueryParser.Match_clauseContext ctx) 
	{
//		System.out.println("match: " + ctx.getText());
		String var, label, from, to;

		for (int i = 0; i < ctx.hop_or_terms().hop_or_term().size(); i++) {
			if (ctx.hop_or_terms().hop_or_term(i).term() == null) { // hop
				HopContext hopCtx = ctx.hop_or_terms().hop_or_term(i).hop();
				// from
				from = hopCtx.term(0).term_body().var().getText();
				if (hopCtx.term(0).term_body().label() != null) {
					label = hopCtx.term(0).term_body().label().getText();
					transRule.getNodeVarToLabelMap().put(from, Util.addQuotes(label));
				} else {
					if (transRule.getNodeVarToLabelMap().containsKey(from) == false) {
						throw new IllegalArgumentException("from[" + from + "] has no label");
					}
//					transRule.getNodeVarToLabelMap().put(from, null);
				}
				if (hopCtx.term(0).LEFTCPAREN() != null) {
					transRule.addVarToStarVarSet(from);
				}

				// to
				to = hopCtx.term(2).term_body().var().getText();
				if (hopCtx.term(2).term_body().label() != null) {
					label = hopCtx.term(2).term_body().label().getText();	
//					System.out.println("to: " + to + " label: " + label);
					transRule.getNodeVarToLabelMap().put(to, Util.addQuotes(label));
				} else {
					if (transRule.getNodeVarToLabelMap().containsKey(to) == false) {
						throw new IllegalArgumentException("to[" + to + "] has no label");						
					}
				}
				if (hopCtx.term(2).LEFTCPAREN() != null) {
					transRule.addVarToStarVarSet(to);
				}

				// via
				var = hopCtx.term(1).term_body().var().getText();
				label = hopCtx.term(1).term_body().label().getText();
				Atom a = new Atom(Config.predE);
				a.appendTerm(new Term(var, true));
				a.appendTerm(new Term(from, true));
				a.appendTerm(new Term(to, true));
				a.appendTerm(new Term(Util.addQuotes(label), false));
				transRule.addAtomToPatternBefore(a); // for typechecking
	
//				System.out.println("MATCH a: " + a);

				a = new Atom(Config.predE);
				a.appendTerm(new Term(var, true));
				a.appendTerm(new Term(from, true));
				a.appendTerm(new Term(to, true));
				a.appendTerm(new Term(Util.addQuotes(label), false));
				
				transRule.addAtomToPatternMatch(a);
			} else { // term
				Term_bodyContext termCtx = ctx.hop_or_terms().hop_or_term(i).term().term_body();
				
				var = termCtx.var().getText();
				if (ctx.hop_or_terms().hop_or_term(i).term().LEFTCPAREN() != null) {
					transRule.addVarToStarVarSet(var);
				}
				if (termCtx.label() != null) {
					label = termCtx.label().getText();
					transRule.getNodeVarToLabelMap().put(var, Util.addQuotes(label));
				} else {
					transRule.getNodeVarToLabelMap().put(var, var + "_label");
//					throw new IllegalArgumentException("Single node[" + var + "] should have a label");	
				}
			}
		}

		// node
		for (HashMap.Entry<String,String> e : transRule.getNodeVarToLabelMap().entrySet()) {
			Atom a = new Atom(Config.predN);
			a.appendTerm(new Term(e.getKey(), true));
			if (e.getValue() == null) {
				a.appendTerm(new Term(e.getKey() + "_label", true));
			} else {
				a.appendTerm(new Term(e.getValue(), false));
			}
			transRule.addAtomToPatternBefore(a);

			a = new Atom(Config.predN);
			a.appendTerm(new Term(e.getKey(), true));
			a.appendTerm(new Term(e.getValue(), false));

			transRule.addAtomToPatternMatch(a);
		}
		
//		System.out.println("nodeVarToLabelMap: " + nodeVarToLabelMap);
		return visitChildren(ctx); 
	}

	@Override 
	public Void visitMap_clause(GraphTransQueryParser.Map_clauseContext ctx) {
		for (int i = 0; i < ctx.maps().map().size(); i++) {
			String var = ctx.maps().map(i).term().term_body().var().getText();
			String label = ctx.maps().map(i).term().term_body().label().getText();
			Atom atom = new Atom(Config.predN);
			atom.appendTerm(new Term(var, true));
			atom.appendTerm(new Term(Util.addQuotes(label), false));

			HashSet<String> set = new HashSet<String>();
			
			GraphTransQueryParser.VarsContext vars = ctx.maps().map(i).vars();
			
			for (int j = 0; j < vars.var_or_setvar().size(); j++) {
				if (vars.var_or_setvar(j).setvar() != null) {
					String setVar = vars.var_or_setvar(j).setvar().var().getText();
					set.add(setVar);
					transRule.addVarToStarVarSet(setVar);
				} else {
					set.add(vars.var_or_setvar(j).var().getText());
				}
			}
			transRule.addMapMap(atom, set);		
			
			transRule.getMetaLabelToVarMap().put(label,  var);
//			System.out.println("atom: " + atom + " set: " + set + " var: " + var + " label: " + label);
		}
//		transRule.show();
		return visitChildren(ctx); 
	}

	@Override 
	public Void visitAdd_clause(GraphTransQueryParser.Add_clauseContext ctx) {
		for (int i = 0; i < ctx.term_or_hops().term_or_hop().size(); i++) {
			if (ctx.term_or_hops().term_or_hop(i).term() == null) { // hop (edge)
				String from = ctx.term_or_hops().term_or_hop(i).hop().term(0).term_body().var().getText();
				String var = ctx.term_or_hops().term_or_hop(i).hop().term(1).term_body().var().getText();
				String label = ctx.term_or_hops().term_or_hop(i).hop().term(1).term_body().label().getText();
				String to = ctx.term_or_hops().term_or_hop(i).hop().term(2).term_body().var().getText();

				Atom atom = new Atom(Config.predE);
				atom.appendTerm(new Term(var, true));
				atom.appendTerm(new Term(from, true));
				atom.appendTerm(new Term(to, true));
				atom.appendTerm(new Term(Util.addQuotes(label), false));

				transRule.getMetaEdgeLabel().add(label);
				transRule.addAtomToPatternAdd(atom);
				
				transRule.getMetaSet().add(var);
				
				transRule.getMetaLabelToVarMap().put(label,  var);

			} else { // term (node)
				String var = ctx.term_or_hops().term_or_hop(i).term().term_body().var().getText();
				String label = ctx.term_or_hops().term_or_hop(i).term().term_body().label().getText();

				Atom atom = new Atom(Config.predN);
				atom.appendTerm(new Term(var, true));
				if (transRule.getNodeVarToLabelMap().containsKey(var) == true) {
					label = transRule.getNodeVarToLabelMap().get(var);
				}
				atom.appendTerm(new Term(Util.addQuotes(label), false));
				transRule.getNodeVarToLabelMap().put(var, label);
				transRule.addAtomToPatternAdd(atom);			
				transRule.getMetaNodeLabel().add(label);
				
				transRule.getMetaSet().add(var);
				
				transRule.getMetaLabelToVarMap().put(label,  var);

			}
		}
		return visitChildren(ctx); 
	}

	@Override 
	public Void visitRemove_clause(GraphTransQueryParser.Remove_clauseContext ctx) { 
		for (int i = 0; i < ctx.term_or_hops().term_or_hop().size(); i++) {
			if (ctx.term_or_hops().term_or_hop(i).term() == null) { // hop (edge)
				String from = ctx.term_or_hops().term_or_hop(i).hop().term(0).term_body().var().getText();
				String var = ctx.term_or_hops().term_or_hop(i).hop().term(1).term_body().var().getText();
				String label = ctx.term_or_hops().term_or_hop(i).hop().term(1).term_body().label().getText();
				String to = ctx.term_or_hops().term_or_hop(i).hop().term(2).term_body().var().getText();

				Atom atom = new Atom(Config.predE);
				atom.appendTerm(new Term(var, true));
				atom.appendTerm(new Term(from, true));
				atom.appendTerm(new Term(to, true));
				atom.appendTerm(new Term(Util.addQuotes(label), false));

				transRule.addAtomToPatternRemove(atom);
			} else { // term (node)
				String var = ctx.term_or_hops().term_or_hop(i).term().term_body().var().getText();
				String label = "*"; // ctx.term_or_hops().term_or_hop(i).term().label().getText();

				Atom atom = new Atom(Config.predN);
				atom.appendTerm(new Term(var, true));
				
				if (transRule.getNodeVarToLabelMap().containsKey(var) == true) {
					label = Util.removeQuotes(transRule.getNodeVarToLabelMap().get(var));
				}
				atom.appendTerm(new Term(Util.addQuotes(label), false));

				transRule.addAtomToPatternRemove(atom);				
			}
		}

		return visitChildren(ctx); 
	}

}
