#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
#include "QueryTreeNode.h"

extern "C" {
    int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

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
typedef map<string, AndList> booleanMap;

class QueryPlanner{
public:
    vector<char *> tableNames;
    vector<char *> joinOrder;
    vector<char *> buffer;
    AliaseMap aliasMap;
    SchemaMap map;
    Statistics s;
    RelOpNode *root;
    
    QueryPlanner();
    void PopulateSchemaMap ();
    void PopulateStatistics ();
    void PopulateAliasMapAndCopyStatistics ();
    void CopyNameList(NameList *nameList, vector<string> &names) ;
    void Compile();
    void Optimise();
    void BuildExecutionTree();
    void Print();
    booleanMap GetMapFromBoolean(AndList *boolean);
    //    void PrintFunction (FuncOperator *func);
    //    void PrintNameList(NameList *nameList);
    //    void PrintParseTree(struct AndList *andPointer);
    //    void PrintTablesAliases (TableList * tableList);

    
};
 QueryPlanner::QueryPlanner(): buffer (2){
    PopulateSchemaMap ();
    PopulateStatistics ();
    cout << "SQL>>" << endl;;
    yyparse ();
};

void QueryPlanner ::PopulateSchemaMap () {

    map[string(region)] = Schema ("catalog", region);
    map[string(part)] = Schema ("catalog", part);
    map[string(partsupp)] = Schema ("catalog", partsupp);
    map[string(nation)] = Schema ("catalog", nation);
    map[string(customer)] = Schema ("catalog", customer);
    map[string(supplier)] = Schema ("catalog", supplier);
    map[string(lineitem)] = Schema ("catalog", lineitem);
    map[string(orders)] = Schema ("catalog", orders);

}

void QueryPlanner::PopulateStatistics () {

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
//
//void QueryPlanner ::PrintParseTree (struct AndList *andPointer) {
//
//    cout << "(";
//
//    while (andPointer) {
//        struct OrList *orPointer = andPointer->left;
//        while (orPointer) {
//            struct ComparisonOp *comPointer = orPointer->left;
//            if (comPointer!=NULL) {
//                struct Operand *pOperand = comPointer->left;
//                if(pOperand!=NULL) {
//                    cout<<pOperand->value<<"";
//                }
//
//                switch(comPointer->code) {
//                    case LESS_THAN:
//                        cout<<" < "; break;
//                    case GREATER_THAN:
//                        cout<<" > "; break;
//                    case EQUALS:
//                        cout<<" = "; break;
//                    default:
//                        cout << " unknown code " << comPointer->code;
//                }
//
//                pOperand = comPointer->right;
//
//                if(pOperand!=NULL) {
//                    cout<<pOperand->value<<"";
//                }
//            }
//
//            if(orPointer->rightOr) {
//                cout<<" OR ";
//            }
//
//            orPointer = orPointer->rightOr;
//        }
//
//        if(andPointer->rightAnd) {
//            cout<<") AND (";
//        }
//
//        andPointer = andPointer->rightAnd;
//    }
//
//    cout << ")" << endl;
//}
//void QueryPlanner::PrintTablesAliases (TableList * tableList)    {
//
//    while (tableList) {
//        cout << "Table " << tableList->tableName;
//        cout <<    " is aliased to " << tableList->aliasAs << endl;
//        tableList = tableList->next;
//    }
//
//}
void QueryPlanner::PopulateAliasMapAndCopyStatistics(){
    while (tables) {
        s.CopyRel (tables->tableName, tables->aliasAs);
        aliasMap[tables->aliasAs] = tables->tableName;
        tableNames.push_back (tables->aliasAs);
        tables = tables->next;
    }
}

//void QueryPlanner ::PrintNameList(NameList *nameList) {
//    while (nameList) {
//        cout << nameList->name << endl;
//        nameList = nameList->next;
//    }
//
//}

void QueryPlanner ::CopyNameList(NameList *nameList, vector<string> &names) {
    while (nameList) {
        names.push_back (string (nameList->name));
        nameList = nameList->next;
    }
}
//
//void QueryPlanner ::PrintFunction (FuncOperator *func) {
//    if (func) {
//        cout << "(";
//        PrintFunction (func->leftOperator);
//        cout << func->leftOperand->value << " ";
//        if (func->code) {
//            cout << " " << func->code << " ";
//        }
//        PrintFunction (func->right);
//        cout << ")";
//    }
//}

void QueryPlanner::Compile(){
    PopulateAliasMapAndCopyStatistics();
    Optimise();
    BuildExecutionTree();
}

void QueryPlanner::Optimise(){
    sort (tableNames.begin (), tableNames.end ());

    double min_join_cost = (double)INT_MAX;
    double curr_join_cost = 0;
    booleanMap b = GetMapFromBoolean(boolean);

    do {
            // code for printing tables
            for (int i=0; i<tableNames.size(); i++) {
                cout << tableNames.at(i)<<",";
            }
            cout << endl;

            Statistics temp (s);
            auto iter = tableNames.begin ();
            buffer[0] = *iter;
            iter++;

            while (iter != tableNames.end ()) {
                
                buffer[1] = *iter;
                string key = string(buffer[0])+","+string(buffer[1]);
                
                // check if this combination is in booleanMap
                if (b.find(key) == b.end()) {
                    break;
                }
                
                curr_join_cost += temp.Estimate (&b[key], &buffer[0], 2);
               
            
                cout << "current join cost " << curr_join_cost << endl;

                temp.Apply (&b[key], &buffer[0], 2);

                if (curr_join_cost <= 0 || curr_join_cost > min_join_cost) {
                    break;
                }

                iter++;
            }

            if (curr_join_cost > 0 && curr_join_cost < min_join_cost) {

                min_join_cost = curr_join_cost;
                joinOrder = tableNames;

            }
            curr_join_cost = 0;

    } while (next_permutation (tableNames.begin (), tableNames.end ()));

    if (joinOrder.size()==0){
        joinOrder = tableNames;
    }
}

void QueryPlanner :: BuildExecutionTree(){

        auto iter = joinOrder.begin ();
        SelectFileOpNode *selectFileNode = new SelectFileOpNode ();

        char filepath[50];
        selectFileNode->opened = true;
        selectFileNode->pid = getPid ();
        selectFileNode->schema = Schema (map[aliasMap[*iter]]);
        selectFileNode->schema.ResetSchema (*iter);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

        iter++;
        if (iter == joinOrder.end ()) {
            root = selectFileNode;
        } 
        else {

            JoinOpNode *joinNode = new JoinOpNode ();
            joinNode->pid = getPid ();
            joinNode->left = selectFileNode;

            selectFileNode = new SelectFileOpNode ();
            selectFileNode->opened = true;
            selectFileNode->pid = getPid ();
            selectFileNode->schema = Schema (map[aliasMap[*iter]]);
            selectFileNode->schema.ResetSchema (*iter);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

            joinNode->right = selectFileNode;
            joinNode->schema.GetSchemaForJoin (joinNode->left->schema, joinNode->right->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

            iter++;

            while (iter != joinOrder.end ()) {

                JoinOpNode *p = joinNode;
                selectFileNode = new SelectFileOpNode ();
                selectFileNode->opened = true;
                selectFileNode->pid = getPid ();
                selectFileNode->schema = Schema (map[aliasMap[*iter]]);
                selectFileNode->schema.ResetSchema (*iter);
                selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

                joinNode = new JoinOpNode ();
                joinNode->pid = getPid ();
                joinNode->left = p;
                joinNode->right = selectFileNode;
                joinNode->schema.GetSchemaForJoin (joinNode->left->schema, joinNode->right->schema);
                joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

                iter++;
            }
            root = joinNode;
        }

        RelOpNode *temp = root;

        if (groupingAtts) 
        {
            if (distinctFunc) {
                root = new DistinctOpNode ();
                root->pid = getPid ();
                root->schema = temp->schema;
                ((DistinctOpNode *) root)->from = temp;
                temp = root;
            }

            root = new GroupByOpNode ();

            vector<string> groupAtts;
            CopyNameList (groupingAtts, groupAtts);

            root->pid = getPid ();
            ((GroupByOpNode *) root)->compute.GrowFromParseTree (finalFunction, temp->schema);
            vector<string> atts;
            CopyNameList (groupingAtts, atts);
            root->schema.GetSchemaForGroup (temp->schema, ((GroupByOpNode *) root)->compute.ReturnInt (),atts);
            ((GroupByOpNode *) root)->group.growFromParseTree (groupingAtts, &(temp->schema));
            ((GroupByOpNode *) root)->from = temp;

        } 
        else if (finalFunction) 
        {

            root = new SumOpNode ();

            root->pid = getPid ();
            ((SumOpNode *) root)->compute.GrowFromParseTree (finalFunction, temp->schema);

            Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
            root->schema = Schema (NULL, 1, ((SumOpNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);

            ((SumOpNode *) root)->from = temp;
        }
        else if (attsToSelect)
        {

            root = new ProjectOpNode ();

            vector<int> attsToKeep;
            vector<string> atts;
            CopyNameList (attsToSelect, atts);

            root->pid = getPid ();
            root->schema.GetSchemaForProject (temp->schema, atts, attsToKeep);
            ((ProjectOpNode *) root)->attsToKeep = &attsToKeep[0];
            ((ProjectOpNode *) root)->numIn = temp->schema.GetNumAtts ();
            ((ProjectOpNode *) root)->numOut = atts.size ();

            ((ProjectOpNode *) root)->from = temp;

        }
}

booleanMap QueryPlanner::GetMapFromBoolean(AndList *parseTree) {
    booleanMap b;
    string delimiter = ".";

    // now we go through and build the comparison structure
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
        
        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            // done
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                // done with parsing
                break;
            }

            // we have not run off the list, so add the current OR in!
            
            // these store the types of the two values that are found
            Type typeLeft;
            Type typeRight;

            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {
                if (myOr->left->right->code == NAME) 
                {
                    cout<<"( "<<myOr->left->left->value<<" ,"<< myOr->left->right->value<<")"<<endl;
                    
                    string key;

                    // left table string
                    string lts = myOr->left->left->value;
                    string pushlts = lts.substr(0, lts.find(delimiter));

                    // right table string
                    string rts = myOr->left->right->value;
                    string pushrts = rts.substr(0, rts.find(delimiter));
                    
                    key=pushlts+","+pushrts;

                    // CNF String
                    string cnfString = "("+string(myOr->left->left->value)+" = "+string(myOr->left->right->value)+")";

                    AndList pushAndList;
                    pushAndList.left=parseTree->left;
                    pushAndList.rightAnd=NULL;

                    b[key] = pushAndList;
                } 
                else if (myOr->left->right->code == STRING  || 
                        myOr->left->right->code == INT      ||
                        myOr->left->right->code == DOUBLE) 
                {
                    continue;
                } 
                else 
                {
                    cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                    //return -1;
                }
            } 
			else if (myOr->left->left->code == STRING   || 
                    myOr->left->left->code == INT       || 
                    myOr->left->left->code == DOUBLE)   
            {
                continue;
            }
            // catch-all case
            else 
            {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                //return -1;
            }

            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr<< "ERROR! Type mismatch in Boolean  " 
                << myOr->left->left->value << " and "
                << myOr->left->right->value << " were found to not match.\n";
                
            }        
        }
    }
    return b;
}

void QueryPlanner:: Print(){
    cout << "Parse Tree : " << endl;
    root->PrintNode();
}

#endif
