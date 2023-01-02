/**
 * Define a grammar called GraphTransQuery
 */
grammar GraphTransQuery;
/*
connect logicblox
 disconnect (auto)
 
 create graph G // create a new workspace, and node,edge,nodeP,edgeP,schema,egds 
 use G; // load workspace (graph G and corresponding views)
 
 // create schema
 create schema node (nodename) (attributes);
 create schema edge (edgename) (FROM vertex_type_name, TO vertex_type_name);

 // add egds
 add constraint N(a1,b,c),N(a2,c,d) -> a1 = a2.

 // import data
 load graph data into G (just believe it satisfies S and C)
 insert node (nid,...) // or nodeProp
 insert edge (eid,...)
 
 // create views
 create materialized view MV (on V') with [R] - typechecking
 create (virtual) view VV (on V') with [R]
 create index view V - remove block and add block?
 (maybe over V)
 
 drop ... view V
 
 query over view V (or base graph G)
*/

cmd: quit | schema | egds | views | program | query | load_script |
	connect | disconnect | create_graph | use_graph | drop_graph | 
	create_schema | add_constraint |
	create_view | user_query | import_data | user_query |
	insert | option | list | prepare_database | create_ssr;

//	| platform | server_start | server_stop | 

// Platform
//platform: 'platform' platform_name;
platform_name: 'lb' | 'pg' | 'n4' | 'sd' ;
list: 'list';

// server
//server_start: 'server' 'start';
//server_stop: 'server' 'stop';

prepare_database: 'prepare' 'from' filepath 'on' platform_name;

// Option
option: 'option' option_name (opt_on_off)?;
option_name : opt_typecheck | opt_prunetypecheck | opt_prunequery | opt_ivm;

opt_typecheck: 'typecheck';
opt_prunetypecheck: 'prunetypecheck';
opt_prunequery: 'prunequery';
opt_ivm: 'ivm';

opt_on_off: opt_on | opt_off;
opt_on: 'on';
opt_off: 'off';

// Commands
load_script: 'load' filepath;
/*
 MATCH (a:Activity)-[r:Used]->(e:MetaEntity)
 WHERE a.id < 1000, e.id = 400
 RETURN (a)-[r]->(e) 
 
 a,r,e
 */
query: 'query';
program: 'program';
views: 'views';
insert: 'insert' rel_name insert_body;
insert_body: '(' int_or_literal (',' int_or_literal)* ')';

import_data: 'import' rel_name 'from' filepath;

rel_name: ID; //('E' | 'N' | 'NP' | 'EP');
filepath: STRING; 

quit: 'quit';
schema: 'schema';
egds: 'egds';
connect: 'connect' platform_name;
disconnect: 'disconnect';
create_graph: 'create' 'graph' ID ('from' filepath)?;
drop_graph: 'drop' ID;
use_graph: 'use' ID;
create_schema: 'create' (node_schema | edge_schema); 
add_constraint: 'add' 'constraint' egd_formula;
create_view: 'create' view_type? 'view' view_name (view_base)? 'as' view_definition;
view_type: 'virtual' | 'materialized' | 'hybrid' | 'asr';
create_index: 'create' 'index' 'on' view_name;
create_sindex: 'create' 'sindex' 'on' view_name;
create_ssindex: 'create' 'ssindex' 'on' view_name;
create_ssr: 'create' 'ssr' 'on' view_name;
//create_asr: 'create' 'asr' 'on' view_name;

/*

match a:A-r:R->b:B
from v2
return a-r->b

match a:A-r:R->b:B
from v2
where a.val = 10, b.v >= "10", c ="A", c.r>a.b
return a
*/
user_query: match_clause from_clause? where_clause? return_clause;

// clauses for user_query
//match_clause2: 'match' hop_or_terms;
hop_or_terms: hop_or_term (',' hop_or_term)*;
hop_or_term: hop | term;

where_clause: 'where' where_conditions;
where_conditions: where_condition (',' where_condition)*;
where_condition: lop operator rop;
lop: var ('.' prop)?;
rop: var ('.' prop)? | propValue;
operator: '=' | '>' | '<' | '>=' | '<=' | '!=';
prop : ID;
propValue : int_or_literal;

from_clause: 'from' ID;
return_clause: 'return' hop_or_terms;

view_name: ID;
view_base: 'on' view_name;
view_definition: trans_rule_paren (',' trans_rule_paren)*;
trans_rule_paren: '{' trans_rule '}';
trans_rule: (match_clause) (where_clause)? (map_clause)? (add_clause)? (remove_clause)?;

match_clause: 'match' hop_or_terms;
map_clause: 'map' maps; // map (a) to c:D, (a,b,c) to f:E
add_clause: 'add' term_or_hops; // add a:A, b:D, a-b:X->c
remove_clause: 'remove' term_or_hops; //

//hops: hop (',' hop)*; 
hop: term '-' term '->' term;
term: term_body | LEFTCPAREN term_body RIGHTCPAREN; // var (':' (label | star))?;
term_body: var (':' (label | star))?;

star: '*';

maps: map (',' map)*;
map: '(' vars ')' 'to' term;
setvar: 'set(' var ')'; 
var_or_setvar: var | setvar;
vars: var_or_setvar (',' var_or_setvar)*;

term_or_hops: term_or_hop (',' term_or_hop)*; 
term_or_hop: term | hop;

node_schema: 'node' label; // node_label
edge_schema: 'edge' label '(' label '->' label ')';
egd_formula: egd_lhs '->' egd_rhs ;

egd_lhs: egd_atom (',' egd_atom)*;
egd_atom: node_atom | edge_atom | nodeProp_atom | edgeProp_atom;

node_atom: rel_name '(' node_atom_body ')';
node_atom_body: var ',' var_or_literal; 
nodeProp_atom: rel_name '(' nodeProp_atom_body ')';
nodeProp_atom_body: var ',' literal ',' literal;

edge_atom: rel_name '(' edge_atom_body ')';
edge_atom_body: var ',' var ',' var ',' var_or_literal;
edgeProp_atom: rel_name '(' edgeProp_atom_body ')'; 
edgeProp_atom_body: var ',' literal ',' literal;

egd_rhs: egd_equality (',' egd_equality)*;
egd_equality: operand1 '=' operand2;

var_or_literal: var | literal;
int_or_literal: INTEGER | literal;

operand1: var;
operand2: var | literal; // TODO: integer

/*
 * MATCH
 * MAP
 * CONNECT
 * DISCONNECT
 */

/*
// below is one line sample rule
[{(match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity map {a,b,c}|->ab, {c}|->c connect e<-WasDerivedFrom-ab disconnect e<-Used-f),(match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity map {a,b,c}|->ab, {c}|->c connect e<-WasDerivedFrom-ab disconnect e<-Used-f)},{(match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity map {a,b,c}|->ab, {c}|->c connect e<-WasDerivedFrom-ab disconnect e<-Used-f)}]



[
{
(
match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, 
      c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity
map {a,b,c}|->ab, {c}|->c
connect e<-WasDerivedFrom-ab
disconnect e<-Used-f
)
,
(
match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, 
      c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity
map {a,b,c}|->ab, {c}|->c
connect e<-WasDerivedFrom-ab
disconnect e<-Used-f
)
}
,
{
(
match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, 
      c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity
map {a,b,c}|->ab, {c}|->c
connect e<-WasDerivedFrom-ab
disconnect e<-Used-f
)
}
] 
 */
rules: rule_stmt_set_list | constraints | query_stmt;

rule_stmt_set_list: '[' rule_stmt_set (',' rule_stmt_set)* ']';

rule_stmt_set: '{' rule_stmt_single (',' rule_stmt_single)* '}';

rule_stmt_single: '(' rule_stmt ')';

rule_stmt: (match_stmt) (map_stmt)? (connect_stmt)? (disconnect_stmt)?;

query_stmt: (for_stmt) (return_stmt);

/* 
 * Query Statement
 * for c:C-X->b:B return b-[]->[], []-[]*->c
 * for c:C-X->b:B return c, b
 * for c:C return c-[]->[], []-[]*->c
 */
for_stmt: 'for' query_exprs;
return_stmt: 'return' query_exprs;

query_exprs: query_expr (',' query_expr)*;
query_expr: query_node_typeable_expr | query_edge_expr;
query_edge_expr: query_node_typeable_expr '-' query_edge_type '->' query_node_typeable_expr;
query_node_typeable_expr: node_typeable_expr | '[]';
query_edge_type: '[]' | '[]*' | type;


/*
 Test Constraints
	constraints { (from:ENTITY -> edge:USED, to:ACTIVITY), (to:ENTITY -> edge:USED, from:ACTIVITY)}
 */
 
constraints: 'constraints' '{' constraint_single (',' constraint_single)* '}';
constraint_single: '(' constraint_determinant '->' constraint_dependent ')';
constraint_node: ('from' | 'to') ':' type;
constraint_edge: 'edge' ':' type;
constraint_determinant: constraint_node;
constraint_dependent: constraint_edge ',' constraint_node;

/* Test Queries
match a:Entity<-used-b:Activity, b<-WasGeneratedBy-c:Entity, 
      c<-Used-d:Activity, d<-WasGeneratedBy-e:Entity, e<-Used-f:Activity
map {a,b,c}|->ab, {c}|->c
connect e<-WasDerivedFrom-ab
disconnect e<-Used-f
*/
match_stmt: 'match' edge_exprs;
map_stmt: 'map' map_exprs;
connect_stmt: 'connect' edge_exprs;
disconnect_stmt: 'disconnect' edge_exprs;

edge_exprs: edge_expr (',' edge_expr)*; 
edge_expr: node_typeable_expr '-' type '->' node_typeable_expr;

node_typeable_expr : node (':' type)? ;

map_exprs: map_expr (',' map_expr)*;

map_expr : '{' nodes '}' '|->' node_typeable_expr ;


nodes : node (',' node)*;

var: ID;
node: ID;
type: ID;
literal: STRING;

label: ID;
port: INTEGER;
integer: INTEGER;

ip: INTEGER (PERIOD INTEGER)*;

// https://github.com/leonchen83/antlr4-mysql/blob/master/MySQL.g4
number:	(PLUS | MINUS)? (INTEGER) ;

PERIOD: '.';
PLUS: '+';
MINUS: '-';
INTEGER : [0-9]+;
LEFTCPAREN: '{';
RIGHTCPAREN: '}';


//IP : [0-9.]+; // not precise

ID : [_a-zA-Z][_a-zA-Z0-9]* ; // match lower-case identifiers

//COMMENT: '//' ~[\r\n]* -> skip ;

WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines

STRING : '"' [-.:/_a-zA-Z0-9]* '"';


