

#include "QueryPlanner.h"



using namespace std;




int main () {
	
	cout << "SQL>>" << endl;;
	yyparse ();

	QueryPlanner qp;
	qp.printDataStructures();
	qp.initSchemaMap();
	qp.initStatistics();
	qp.CopyTablesNamesAndAliases ();
	sort (qp.tableNames.begin (), qp.tableNames.end ());
	qp.executePlan();
}
