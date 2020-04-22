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

class QueryPlanner{
public:
    vector<char *> tableNames;
    vector<char *> joinOrder;
    vector<char *> buffer;
    AliaseMap aliaseMap;
    SchemaMap map;
    Statistics s;
    RelOpNode *root;
    
    QueryPlanner();
    void initSchemaMap ();
    void initStatistics ();
    void PrintParseTree(struct AndList *andPointer);
    void PrintTablesAliases (TableList * tableList)    ;
    void CopyTablesNamesAndAliases ()    ;
    void PrintNameList(NameList *nameList) ;
    void CopyNameList(NameList *nameList, vector<string> &names) ;
    void PrintFunction (FuncOperator *func) ;
    void Compile();
    void Optimise();
    void BuildExecutionTree();
    void Print();

    
};
 QueryPlanner::QueryPlanner(): buffer (2){
    initSchemaMap ();
    initStatistics ();
};

void QueryPlanner ::initSchemaMap () {

    map[string(region)] = Schema ("catalog", region);
    map[string(part)] = Schema ("catalog", part);
    map[string(partsupp)] = Schema ("catalog", partsupp);
    map[string(nation)] = Schema ("catalog", nation);
    map[string(customer)] = Schema ("catalog", customer);
    map[string(supplier)] = Schema ("catalog", supplier);
    map[string(lineitem)] = Schema ("catalog", lineitem);
    map[string(orders)] = Schema ("catalog", orders);

}

void QueryPlanner::initStatistics () {

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

void QueryPlanner ::PrintParseTree (struct AndList *andPointer) {

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
void QueryPlanner::PrintTablesAliases (TableList * tableList)    {

    while (tableList) {

        cout << "Table " << tableList->tableName;
        cout <<    " is aliased to " << tableList->aliasAs << endl;

        tableList = tableList->next;

    }

}
void QueryPlanner::CopyTablesNamesAndAliases(){
    while (tables) {
        s.CopyRel (tables->tableName, tables->aliasAs);
        aliaseMap[tables->aliasAs] = tables->tableName;
        tableNames.push_back (tables->aliasAs);
        tables = tables->next;
    }
}

void QueryPlanner ::PrintNameList(NameList *nameList) {

    while (nameList) {

        cout << nameList->name << endl;

        nameList = nameList->next;

    }

}

void QueryPlanner ::CopyNameList(NameList *nameList, vector<string> &names) {

    while (nameList) {

        names.push_back (string (nameList->name));

        nameList = nameList->next;

    }

}

void QueryPlanner ::PrintFunction (FuncOperator *func) {

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

void QueryPlanner::Compile(){
    cout << "SQL>>" << endl;;
    yyparse ();
    CopyTablesNamesAndAliases();
    Optimise();
    BuildExecutionTree();

}

void QueryPlanner::Optimise(){

        sort (tableNames.begin (), tableNames.end ());

        int minCost = INT_MAX, cost = 0;
        int counter = 1;

        do {

            Statistics temp (s);

            auto iter = tableNames.begin ();
            buffer[0] = *iter;

    //        cout << *iter << " ";
            iter++;

            while (iter != tableNames.end ()) {

    //            cout << *iter << " ";
                buffer[1] = *iter;

                cost += temp.Estimate (boolean, &buffer[0], 2);
                temp.Apply (boolean, &buffer[0], 2);

                if (cost <= 0 || cost > minCost) {

                    break;

                }

                iter++;

            }

    //        cout << endl << cost << endl;
    //        cout << counter++ << endl << endl;

            if (cost > 0 && cost < minCost) {

                minCost = cost;
                joinOrder = tableNames;

            }
    /*
            char fileName[10];
            sprintf (fileName, "t%d.txt", counter - 1);
            temp.Write (fileName);
    */
            cost = 0;

        } while (next_permutation (tableNames.begin (), tableNames.end ()));

        if (joinOrder.size()==0){
            joinOrder = tableNames;
        }


}

void QueryPlanner :: BuildExecutionTree(){
    //    cout << minCost << endl;

        auto iter = joinOrder.begin ();
        SelectFileOpNode *selectFileNode = new SelectFileOpNode ();

        char filepath[50];
    //    sprintf (filepath, "bin/%s.bin",aliaseMap[*iter]);

    //    selectFileNode->file.Open (filepath);
        selectFileNode->opened = true;
        selectFileNode->pid = getPid ();
        selectFileNode->schema = Schema (map[aliaseMap[*iter]]);
        selectFileNode->schema.Reset (*iter);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

        iter++;
        if (iter == joinOrder.end ()) {

            root = selectFileNode;

        } else {

            JoinOpNode *joinNode = new JoinOpNode ();

            joinNode->pid = getPid ();
            joinNode->left = selectFileNode;

            selectFileNode = new SelectFileOpNode ();

    //        sprintf (filepath, "bin\\%s.bin", aliaseMap[*iter]);

    //        selectFileNode->file.Open (filepath);
            selectFileNode->opened = true;
            selectFileNode->pid = getPid ();
            selectFileNode->schema = Schema (map[aliaseMap[*iter]]);

            selectFileNode->schema.Reset (*iter);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

            joinNode->right = selectFileNode;
            joinNode->schema.JoinSchema (joinNode->left->schema, joinNode->right->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

            iter++;

            while (iter != joinOrder.end ()) {

                JoinOpNode *p = joinNode;

                selectFileNode = new SelectFileOpNode ();

    //            sprintf (filepath, "bin/%s.bin", (aliaseMap[*iter]));
    //            selectFileNode->file.Open (filepath);
                selectFileNode->opened = true;
                selectFileNode->pid = getPid ();
                selectFileNode->schema = Schema (map[aliaseMap[*iter]]);
                selectFileNode->schema.Reset (*iter);
                selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->literal);

                joinNode = new JoinOpNode ();

                joinNode->pid = getPid ();
                joinNode->left = p;
                joinNode->right = selectFileNode;

                joinNode->schema.JoinSchema (joinNode->left->schema, joinNode->right->schema);
                joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->left->schema), &(joinNode->right->schema), joinNode->literal);

                iter++;

            }

            root = joinNode;

        }

        RelOpNode *temp = root;

        if (groupingAtts) {

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
            root->schema.GroupBySchema (temp->schema, ((GroupByOpNode *) root)->compute.ReturnInt ());
            ((GroupByOpNode *) root)->group.growFromParseTree (groupingAtts, &(root->schema));

            ((GroupByOpNode *) root)->from = temp;

        } else if (finalFunction) {

            root = new SumOpNode ();

            root->pid = getPid ();
            ((SumOpNode *) root)->compute.GrowFromParseTree (finalFunction, temp->schema);

            Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
            root->schema = Schema (NULL, 1, ((SumOpNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);

            ((SumOpNode *) root)->from = temp;

        }
        temp= root;
        if (attsToSelect) {

            root = new ProjectOpNode ();

            vector<int> attsToKeep;
            vector<string> atts;
            CopyNameList (attsToSelect, atts);

            root->pid = getPid ();
            root->schema.ProjectSchema (temp->schema, atts, attsToKeep);
            ((ProjectOpNode *) root)->attsToKeep = &attsToKeep[0];
            ((ProjectOpNode *) root)->numIn = temp->schema.GetNumAtts ();
            ((ProjectOpNode *) root)->numOut = atts.size ();

            ((ProjectOpNode *) root)->from = temp;

        }
}

void QueryPlanner:: Print(){

    cout << "Parse Tree : " << endl;
    root->PrintNode();
}

#endif
