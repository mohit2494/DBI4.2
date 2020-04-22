#ifndef QUERYTREENODE_H
#define QUERYTREENODE_H
enum RelOpType {
    G, SF, SP, P, D, S, GB, J, W
};

class RelOpNode {

public:

    int pid;
    RelOpType t;
    Schema schema;
    RelOpNode ();
    RelOpNode (RelOpType type) : t (type) {}
    ~RelOpNode () {}
    virtual void PrintNode () {};

};

class JoinOpNode : public RelOpNode {

public:

    RelOpNode *left;
    RelOpNode *right;
    CNF cnf;
    Record literal;

    JoinOpNode () : RelOpNode (J) {}
    ~JoinOpNode () {

        if (left) delete left;
        if (right) delete right;

    }

    void PrintNode () {
        left->PrintNode ();
        right->PrintNode ();
        cout << "<------------ Join Op ------------>" << endl;
        cout << " Left Input Pipe ID : " << left->pid << endl;
        cout << " Right Input Pipe ID : " << right->pid << endl;
        cout << " Output Pipe ID : " << pid << endl;
        cout << " Output Schema : " << endl;
        schema.Print ();
        cout << " Join CNF : " << endl;
        cnf.PrintWithSchema(&left->schema,&right->schema,&literal);
        cout << "<----------------XX---------------->" << endl;

    }

};

class ProjectOpNode : public RelOpNode {

public:

    int numIn;
    int numOut;
    int *attsToKeep;

    RelOpNode *from;

    ProjectOpNode () : RelOpNode (P) {}
    ~ProjectOpNode () {

        if (attsToKeep) delete[] attsToKeep;

    }

    void PrintNode () {
        from->PrintNode ();
        
        cout << "<------------ Project Op ------------>" << endl;
        cout << " Input Pipe ID : " << from->pid << endl;
        cout << " Output Pipe ID " << pid << endl;
        cout << " Number Attrs Input : " << numIn << endl;
        cout << " Attrs To Keep : [";
        for (int i = 0; i < numOut; i++) {
            cout << attsToKeep[i] <<" ";

        }
        cout<< "]"<< endl;
        cout << "Number Attrs Output : " << numOut << endl;
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "<----------------XX---------------->" << endl;


    }

};

class SelectFileOpNode : public RelOpNode {

public:

    bool opened;

    CNF cnf;
    DBFile file;
    Record literal;

    SelectFileOpNode () : RelOpNode (SF) {}
    ~SelectFileOpNode () {

        if (opened) {

            file.Close ();

        }

    }

    void PrintNode () {

        cout << "<----------- Select File Op ---------->" << endl;
        cout << "Select File Operation" << endl;
        cout << "Output Pipe ID " << pid << endl;
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "Select CNF:" << endl;
        cnf.PrintWithSchema(&schema,&schema,&literal);
        cout << "<-----------------XX---------------->" << endl;

    }

};

class SelectPipeOpNode : public RelOpNode {

public:

    CNF cnf;
    Record literal;
    RelOpNode *from;

    SelectPipeOpNode () : RelOpNode (SP) {}
    ~SelectPipeOpNode () {

        if (from) delete from;

    }

    void PrintNode () {
        from->PrintNode ();
        cout << "*********************" << endl;
        cout << "Select Pipe Operation" << endl;
        cout << "Input Pipe ID : " << from->pid << endl;
        cout << "Output Pipe ID : " << pid << endl;
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "Select CNF:" << endl;
        cnf.PrintWithSchema(&schema,&schema,&literal);
        cout << "*********************" << endl;

    }

};

class SumOpNode : public RelOpNode {

public:

    Function compute;
    RelOpNode *from;

    SumOpNode () : RelOpNode (S) {}
    ~SumOpNode () {

        if (from) delete from;

    }

    void PrintNode () {

        from->PrintNode ();
        cout << "*********************" << endl;
        cout << "Sum Operation" << endl;
        cout << "Input Pipe ID : " << from->pid << endl;
        cout << "Output Pipe ID : " << pid << endl;
        cout << "Function :" << endl;
        compute.Print ();
        cout << "*********************" << endl;

    }

};

class DistinctOpNode : public RelOpNode {

public:

    RelOpNode *from;

    DistinctOpNode () : RelOpNode (D) {}
    ~DistinctOpNode () {

        if (from) delete from;

    }

    void PrintNode () {
        from->PrintNode ();
        cout << "*********************" << endl;
        cout << "Duplication Elimation Operation" << endl;
        cout << "Input Pipe ID : " << from->pid << endl;
        cout << "Output Pipe ID : " << pid << endl;
        cout << "*********************" << endl;

    }

};

class GroupByOpNode : public RelOpNode {

public:

    RelOpNode *from;

    Function compute;
    OrderMaker group;

    GroupByOpNode () : RelOpNode (GB) {}
    ~GroupByOpNode () {

        if (from) delete from;

    }

    void PrintNode () {

        from->PrintNode ();
        cout << "*********************" << endl;
        cout << "Group By Operation" << endl;
        cout << "Input Pipe ID : " << from->pid << endl;
        cout << "Output Pipe ID : " << pid << endl;
        cout << "Output Schema : " << endl;
        schema.Print ();
        cout << "Function : " << endl;
        compute.Print ();
        cout << "OrderMaker : " << endl;
        group.Print ();
        cout << "*********************" << endl;

    }

};

class WriteOutOpNode : public RelOpNode {

public:

    RelOpNode *from;

    FILE *output;

    WriteOutOpNode () : RelOpNode (W) {}
    ~WriteOutOpNode () {

        if (from) delete from;

    }

    void PrintNode () {

        from->PrintNode ();
        cout << "*********************" << endl;
        cout << "Write Out Operation" << endl;
        cout << "Input Pipe ID : " << from->pid << endl;
        cout << "*********************" << endl;

    }

};
#endif
