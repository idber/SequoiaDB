#include "vergen.h"
#include "ossVer.h"
#include <fstream>
#include <iostream>
#include <sstream>

using namespace std ;

#define VER_MAJOR       "major"
#define VER_MINOR       "minor"

static void genDoc()
{
   #define VER_DOC_PATH    "../../doc/config/version.json"
   ofstream fout( VER_DOC_PATH ) ;

   fout << "{" << endl ;
   fout << "    " << "\"" << VER_MAJOR << "\": " << SDB_ENGINE_VERISON_CURRENT << "," << endl ;
   fout << "    " << "\"" << VER_MINOR << "\": " << SDB_ENGINE_SUBVERSION_CURRENT << endl ;
   fout << "}" << endl ;
}

static void genPython()
{
   #define VER_PY_PATH     "../../driver/python/version.py"
   ofstream fout( VER_PY_PATH ) ;

   fout << "# auto-generated, do not edit!!!" << endl;
   fout << "version = '" 
        << SDB_ENGINE_VERISON_CURRENT << "." << SDB_ENGINE_SUBVERSION_CURRENT
        << "'" << endl;
}

VerGen::VerGen()
{
}

VerGen::~VerGen()
{
}

void VerGen::run(bool doc)
{
   if (doc)
   {
      genDoc();
   }
   else
   {
      genPython();
   }
}

