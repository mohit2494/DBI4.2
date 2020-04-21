#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include <iostream>


enum NodeType {
	G, SF, SP, P, D, S, GB, J, W
};

class QueryNode {

public:
	
	int pid;  // Pipe ID
	
	NodeType t;
	Schema sch;  // Ouput Schema
	
	QueryNode ();
	QueryNode (NodeType type) : t (type) {}
	
	~QueryNode () {}
	virtual void Print () {};
	
};

class JoinNode : public QueryNode {

public:
	
	QueryNode *left;
	QueryNode *right;
	CNF cnf;
	Record literal;
	
	JoinNode () : QueryNode (J) {}
	~JoinNode () {
		
		if (left) delete left;
		if (right) delete right;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Join Operation" << endl;
		cout << "Input Pipe 1 ID : " << left->pid << endl;
		cout << "Input Pipe 2 ID : " << right->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Join CNF : " << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		left->Print ();
		right->Print ();
		
	}
	
};

class ProjectNode : public QueryNode {

public:
	
	int numIn;
	int numOut;
	int *attsToKeep;
	
	QueryNode *from;
	
	ProjectNode () : QueryNode (P) {}
	~ProjectNode () {
		
		if (attsToKeep) delete[] attsToKeep;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Project Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Number Attrs Input : " << numIn << endl;
		cout << "Number Attrs Output : " << numOut << endl;
		cout << "Attrs To Keep :" << endl;
		for (int i = 0; i < numOut; i++) {
			
			cout << attsToKeep[i] << endl;
			
		}
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

class SelectFileNode : public QueryNode {

public:
	
	bool opened;
	
	CNF cnf;
	DBFile file;
	Record literal;
	
	SelectFileNode () : QueryNode (SF) {}
	~SelectFileNode () {
		
		if (opened) {
			
			file.Close ();
			
		}
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Select File Operation" << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
	}
	
};

class SelectPipeNode : public QueryNode {

public:
	
	CNF cnf;
	Record literal;
	QueryNode *from;
	
	SelectPipeNode () : QueryNode (SP) {}
	~SelectPipeNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Select Pipe Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

class SumNode : public QueryNode {

public:
	
	Function compute;
	QueryNode *from;
	
	SumNode () : QueryNode (S) {}
	~SumNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Sum Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Function :" << endl;
		compute.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

class DistinctNode : public QueryNode {

public:
	
	QueryNode *from;
	
	DistinctNode () : QueryNode (D) {}
	~DistinctNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Duplication Elimation Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

class GroupByNode : public QueryNode {

public:
	
	QueryNode *from;
	
	Function compute;
	OrderMaker group;
	
	GroupByNode () : QueryNode (GB) {}
	~GroupByNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Group By Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Function : " << endl;
		compute.Print ();
		cout << "OrderMaker : " << endl;
		group.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

class WriteOutNode : public QueryNode {

public:
	
	QueryNode *from;
	
	FILE *output;
	
	WriteOutNode () : QueryNode (W) {}
	~WriteOutNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Write Out Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
};

#endif