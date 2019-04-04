#ifndef VERSION_GEN_FOR_DRIVER_HPP
#define VERSION_GEN_FOR_DRIVER_HPP

#include "versionGeneratorBase.hpp"

#define VERSION_PYTHON_PATH   DRIVER_PATH"python/version.py"

class versionGenForPython : public versionGeneratorBase
{
DECLARE_GENERATOR_AUTO_REGISTER() ;

public:
   versionGenForPython() ;
   ~versionGenForPython() ;

   bool hasNext() ;
   int outputFile( int id, fileOutStream &fout,
                   string &outputPath ) ;
   const char* name() { return "version for driver" ; }

private:
   int _genPythonFile( fileOutStream &fout, string &outputPath ) ;

private:
   bool _isFinish ;
} ;

#endif
