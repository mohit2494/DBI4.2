#include <string>
#include <fstream>
#include "DBFile.h"
#include "Statistics.h"
#include <gtest/gtest.h>

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

class FilePath{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="/home/mk/Documents/uf docs/sem 2/Database Implementation/git/tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
};


TEST(QueryTesting, estimate) {
   
}

TEST(QueryTesting, apply) {
   
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}