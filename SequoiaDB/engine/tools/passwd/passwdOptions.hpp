#ifndef   PASSWD_OPTOPNS_H_
#define   PASSWD_OPTOPNS_H_

#include "oss.hpp"
#include "ossTypes.h"
#include <iostream>
#include <string>
#include <boost/program_options.hpp>

using namespace std ;

namespace passwd
{
   namespace po = boost::program_options ;

   class passwdOptions : public SDBObject
   {
      public:
         enum OpMode { OpAddUser, OpRemoveUser } ;

         passwdOptions() ;
         ~passwdOptions() ;

         INT32 parseCmd(INT32 argc, CHAR* argv[]) ;
         INT32 printHelpInfo() const ;

         BOOLEAN  hasHelp() const ;
         BOOLEAN  hasPassword() const ;

         inline const string &user()        const { return _user ; }
         inline const string &token()       const { return _token ; }
         inline const string &file()        const { return _file ; }
         inline const string &password()    const { return _password ; }
         inline const OpMode &mode()        const { return _mode ; }

      private:
         BOOLEAN _cmdHas(const CHAR* option) const;

      private:
         BOOLEAN                   _cmdParsed ;
         po::options_description   _cmdDesc ;
         po::variables_map         _cmdVm ;
         string                    _user ;
         string                    _token ;
         OpMode                    _mode ;
         string                    _password ;
         string                    _file ;
   } ;

}
#endif
