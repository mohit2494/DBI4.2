#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include "QueryTreeNode.h"
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

const int ncustomer = 150000;
const int nlineitem = 6001215;
const int nnation = 25;
const int norders = 1500000;
const int npart = 200000;
const int npartsupp = 800000;
const int nregion = 5;
const int nsupplier = 10000;

static int pidBuffer = 0;
int getPid () {
	return ++pidBuffer;
}

typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;

void initSchemaMap (SchemaMap &map) {
	
	map[string(region)] = Schema ("catalog", region);
	map[string(part)] = Schema ("catalog", part);
	map[string(partsupp)] = Schema ("catalog", partsupp);
	map[string(nation)] = Schema ("catalog", nation);
	map[string(customer)] = Schema ("catalog", customer);
	map[string(supplier)] = Schema ("catalog", supplier);
	map[string(lineitem)] = Schema ("catalog", lineitem);
	map[string(orders)] = Schema ("catalog", orders);
	
}

void initStatistics (Statistics &s) {
	
	s.AddRel (region, nregion);
	s.AddRel (nation, nnation);
	s.AddRel (part, npart);
	s.AddRel (supplier, nsupplier);
	s.AddRel (partsupp, npartsupp);
	s.AddRel (customer, ncustomer);
	s.AddRel (orders, norders);
	s.AddRel (lineitem, nlineitem);

	// region
	s.AddAtt (region, "r_regionkey", nregion);
	s.AddAtt (region, "r_name", nregion);
	s.AddAtt (region, "r_comment", nregion);
	
	// nation
	s.AddAtt (nation, "n_nationkey",  nnation);
	s.AddAtt (nation, "n_name", nnation);
	s.AddAtt (nation, "n_regionkey", nregion);
	s.AddAtt (nation, "n_comment", nnation);
	
	// part
	s.AddAtt (part, "p_partkey", npart);
	s.AddAtt (part, "p_name", npart);
	s.AddAtt (part, "p_mfgr", npart);
	s.AddAtt (part, "p_brand", npart);
	s.AddAtt (part, "p_type", npart);
	s.AddAtt (part, "p_size", npart);
	s.AddAtt (part, "p_container", npart);
	s.AddAtt (part, "p_retailprice", npart);
	s.AddAtt (part, "p_comment", npart);
	
	// supplier
	s.AddAtt (supplier, "s_suppkey", nsupplier);
	s.AddAtt (supplier, "s_name", nsupplier);
	s.AddAtt (supplier, "s_address", nsupplier);
	s.AddAtt (supplier, "s_nationkey", nnation);
	s.AddAtt (supplier, "s_phone", nsupplier);
	s.AddAtt (supplier, "s_acctbal", nsupplier);
	s.AddAtt (supplier, "s_comment", nsupplier);
	
	// partsupp
	s.AddAtt (partsupp, "ps_partkey", npart);
	s.AddAtt (partsupp, "ps_suppkey", nsupplier);
	s.AddAtt (partsupp, "ps_availqty", npartsupp);
	s.AddAtt (partsupp, "ps_supplycost", npartsupp);
	s.AddAtt (partsupp, "ps_comment", npartsupp);
	
	// customer
	s.AddAtt (customer, "c_custkey", ncustomer);
	s.AddAtt (customer, "c_name", ncustomer);
	s.AddAtt (customer, "c_address", ncustomer);
	s.AddAtt (customer, "c_nationkey", nnation);
	s.AddAtt (customer, "c_phone", ncustomer);
	s.AddAtt (customer, "c_acctbal", ncustomer);
	s.AddAtt (customer, "c_mktsegment", 5);
	s.AddAtt (customer, "c_comment", ncustomer);
	
	// orders
	s.AddAtt (orders, "o_orderkey", norders);
	s.AddAtt (orders, "o_custkey", ncustomer);
	s.AddAtt (orders, "o_orderstatus", 3);
	s.AddAtt (orders, "o_totalprice", norders);
	s.AddAtt (orders, "o_orderdate", norders);
	s.AddAtt (orders, "o_orderpriority", 5);
	s.AddAtt (orders, "o_clerk", norders);
	s.AddAtt (orders, "o_shippriority", 1);
	s.AddAtt (orders, "o_comment", norders);
	
	// lineitem
	s.AddAtt (lineitem, "l_orderkey", norders);
	s.AddAtt (lineitem, "l_partkey", npart);
	s.AddAtt (lineitem, "l_suppkey", nsupplier);
	s.AddAtt (lineitem, "l_linenumber", nlineitem);
	s.AddAtt (lineitem, "l_quantity", nlineitem);
	s.AddAtt (lineitem, "l_extendedprice", nlineitem);
	s.AddAtt (lineitem, "l_discount", nlineitem);
	s.AddAtt (lineitem, "l_tax", nlineitem);
	s.AddAtt (lineitem, "l_returnflag", 3);
	s.AddAtt (lineitem, "l_linestatus", 2);
	s.AddAtt (lineitem, "l_shipdate", nlineitem);
	s.AddAtt (lineitem, "l_commitdate", nlineitem);
	s.AddAtt (lineitem, "l_receiptdate", nlineitem);
	s.AddAtt (lineitem, "l_shipinstruct", nlineitem);
	s.AddAtt (lineitem, "l_shipmode", 7);
	s.AddAtt (lineitem, "l_comment", nlineitem);
	
}

void PrintParseTree (struct AndList *andPointer) {
  
	cout << "(";
  
	while (andPointer) {
	  
		struct OrList *orPointer = andPointer->left;
      
		while (orPointer) {
		  
			struct ComparisonOp *comPointer = orPointer->left;
			
			if (comPointer!=NULL) {
			
				struct Operand *pOperand = comPointer->left;
				
				if(pOperand!=NULL) {
					
					cout<<pOperand->value<<"";
					
				}
				
				switch(comPointer->code) {
					
					case LESS_THAN:
						cout<<" < "; break;
					case GREATER_THAN:
						cout<<" > "; break;
					case EQUALS:
						cout<<" = "; break;
					default:
						cout << " unknown code " << comPointer->code;
					
				}
				
				pOperand = comPointer->right;
				
				if(pOperand!=NULL) {
					
					cout<<pOperand->value<<"";
				}
				
			}
			
			if(orPointer->rightOr) {
				
				cout<<" OR ";
				
			}
			
			orPointer = orPointer->rightOr;
			
		}
		
		if(andPointer->rightAnd) {
			
			cout<<") AND (";
		}
		
		andPointer = andPointer->rightAnd;
		
	}
	
	cout << ")" << endl;
	
}

void PrintTablesAliases (TableList * tableList)	{
	
	while (tableList) {
		
		cout << "Table " << tableList->tableName;
		cout <<	" is aliased to " << tableList->aliasAs << endl;
		
		tableList = tableList->next;
		
	}
	
}

void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)	{
	
	while (tableList) {
		
		s.CopyRel (tableList->tableName, tableList->aliasAs);
		
		map[tableList->aliasAs] = tableList->tableName;
		
		tableNames.push_back (tableList->aliasAs);
		
		tableList = tableList->next;
		
	}
	
}

void PrintNameList(NameList *nameList) {
	
	while (nameList) {
		
		cout << nameList->name << endl;
		
		nameList = nameList->next;
	
	}
	
}

void CopyNameList(NameList *nameList, vector<string> &names) {
	
	while (nameList) {
		
		names.push_back (string (nameList->name));
		
		nameList = nameList->next;
	
	}
	
}

void PrintFunction (FuncOperator *func) {
	
	if (func) {
		
		cout << "(";
		
		PrintFunction (func->leftOperator);
		
		cout << func->leftOperand->value << " ";
		if (func->code) {
			
			cout << " " << func->code << " ";
		
		}
		
		PrintFunction (func->right);
		
		cout << ")";
		
	}
	
}







#endif