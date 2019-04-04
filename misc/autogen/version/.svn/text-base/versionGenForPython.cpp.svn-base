#include "versionGenForPython.hpp"
#include "ossVer.h"

#if defined (GENERAL_VER_PYTHON_FILE)
IMPLEMENT_GENERATOR_AUTO_REGISTER( versionGenForPython, GENERAL_VER_PYTHON_FILE ) ;
#endif

versionGenForPython::versionGenForPython() : _isFinish( false )
{
}

versionGenForPython::~versionGenForPython()
{
}

bool versionGenForPython::hasNext()
{
   return !_isFinish ;
}

int versionGenForPython::outputFile( int id, fileOutStream &fout,
                                  string &outputPath )
{
   int rc = 0 ;

   if ( id == 0 )
   {
      rc = _genPythonFile( fout, outputPath ) ;
   }

   _isFinish = true ;

   return rc ;
}


int versionGenForPython::_genPythonFile( fileOutStream &fout, string &outputPath )
{
   outputPath = VERSION_PYTHON_PATH ;

   fout << "# auto-generated, do not edit!!!"
        << endl
        << "version = '"
        << SDB_ENGINE_VERISON_CURRENT
        << "." << SDB_ENGINE_SUBVERSION_CURRENT
#ifdef SDB_ENGINE_FIXVERSION_CURRENT
        << "." << SDB_ENGINE_FIXVERSION_CURRENT
#endif
        << "'"
        << endl ;
   return 0 ;
}

