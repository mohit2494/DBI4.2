
#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>


#include "QueryPlanner.h"

extern "C" {
	int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction; 	// the aggregate function (NULL if no agg)
extern struct TableList *tables; 			// the list of tables and aliases in the query
extern struct AndList *boolean; 			// the predicate in the WHERE clause
extern struct NameList *groupingAtts; 		// grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; 		// the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; 					// 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  					// 1 if there is a DISTINCT in an aggregate query



int main () {
	
	cout << "SQL>>" << endl;;
	yyparse ();
	
	// cout << endl << "Print Boolean :" << endl;
	// PrintParseTree (boolean);
	
	// cout << endl << "Print TableList :" << endl;
	// PrintTablesAliases (tables);
	
	// cout << endl << "Print NameList groupingAtts :" << endl;
	// PrintNameList (groupingAtts);
	
	// cout << endl << "Print NameLis1t attsToSelect:" << endl;
	// PrintNameList (attsToSelect);
	
	// cout << finalFunction << endl;
	// cout << endl << "Print Function:" << endl;
	// PrintFunction (finalFunction);
	
	cout << endl;
	
    vector<char *> tableNames;
	vector<char *> joinOrder;
	vector<char *> buffer (2);
	
	AliaseMap aliaseMap;



	SchemaMap schemaMap;
	Statistics s;
	
	initSchemaMap (schemaMap);
	initStatistics (s);
	CopyTablesNamesAndAliases (tables, s, tableNames, aliaseMap);
	
	sort (tableNames.begin (), tableNames.end ());
	
	int minCost = INT_MAX, cost = 0;
	int counter = 1;
	
	do {
		
		Statistics temp (s);
		auto iter = tableNames.begin ();
		buffer[0] = *iter;
		iter++;
		
		while (iter != tableNames.end ()) {
		
			buffer[1] = *iter;
			
			cost += temp.Estimate (boolean, &buffer[0], 2);
			temp.Apply (boolean, &buffer[0], 2);
			
			if (cost <= 0 || cost > minCost) {
				
				break;
				
			}	
			iter++;
		
		}
		
		if (cost >= 0 && cost < minCost) {
			
			minCost = cost;
			joinOrder = tableNames;
			
		}

		cost = 0;
		
	} while (next_permutation (tableNames.begin (), tableNames.end ()));
	
	QueryNode *root;
	
	auto iter = joinOrder.begin ();
	SelectFileNode *selectFileNode = new SelectFileNode ();
	
	char filepath[50];
	selectFileNode->opened = true;
	selectFileNode->pid = getPid ();
	selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
	selectFileNode->sch.Reset (*iter);
	
	selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
	
	iter++;
	if (iter == joinOrder.end ()) {
		
		root = selectFileNode;
		
	} else {
		
		JoinNode *joinNode = new JoinNode ();
		
		joinNode->pid = getPid ();
		joinNode->left = selectFileNode;
		
		selectFileNode = new SelectFileNode ();
		selectFileNode->opened = true;
		selectFileNode->pid = getPid ();
		selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
		selectFileNode->sch.Reset (*iter);
		selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
		
		joinNode->right = selectFileNode;
		joinNode->sch.JoinSchema (joinNode->left->sch, joinNode->right->sch);
		joinNode->cnf.GrowFromParseTree (boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
		
		iter++;
		
		while (iter != joinOrder.end ()) {
			
			JoinNode *p = joinNode;
			
			selectFileNode = new SelectFileNode ();
			selectFileNode->opened = true;
			selectFileNode->pid = getPid ();
			selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
			selectFileNode->sch.Reset (*iter);
			selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
			
			joinNode = new JoinNode ();
			joinNode->pid = getPid ();
			joinNode->left = p;
			joinNode->right = selectFileNode;
			joinNode->sch.JoinSchema (joinNode->left->sch, joinNode->right->sch);
			joinNode->cnf.GrowFromParseTree (boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
			
			iter++;
		}
		root = joinNode;
	}
	
	QueryNode *temp = root;
	
	if (groupingAtts) {
		if (distinctFunc) {

			root = new DistinctNode ();
			root->pid = getPid ();
			root->sch = temp->sch;
			((DistinctNode *) root)->from = temp;
			temp = root;
		}
		
		root = new GroupByNode ();
		
		vector<string> groupAtts;
		CopyNameList (groupingAtts, groupAtts);
		
		root->pid = getPid ();
		((GroupByNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
		root->sch.GroupBySchema (temp->sch, ((GroupByNode *) root)->compute.ReturnInt ());
		((GroupByNode *) root)->group.growFromParseTree (groupingAtts, &(root->sch));
		((GroupByNode *) root)->from = temp;
		
	} 
	else if (finalFunction) {
		
		root = new SumNode ();
		
		root->pid = getPid ();
		((SumNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
		
		Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
		root->sch = Schema (NULL, 1, ((SumNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);
		
		((SumNode *) root)->from = temp;
		
	}
    temp= root;
    if (attsToSelect) 
	{
		
		root = new ProjectNode ();
		
		vector<int> attsToKeep;
		vector<string> atts;
		CopyNameList (attsToSelect, atts);
		
		root->pid = getPid ();
		root->sch.ProjectSchema (temp->sch, atts, attsToKeep);
		((ProjectNode *) root)->attsToKeep = &attsToKeep[0];
		((ProjectNode *) root)->numIn = temp->sch.GetNumAtts ();
		((ProjectNode *) root)->numOut = atts.size ();
		
		((ProjectNode *) root)->from = temp;
		
	}
	
	cout << "Parse Tree : " << endl;
	root->Print ();
	
	return 0;
	
}
