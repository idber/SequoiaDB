/*******************************************************************************

   Copyright (C) 2011-2018 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Source File Name = clsUniqueIDCheckJob.hpp

   Descriptive Name = CS/CL UniqueID Checking Job Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who     Description
   ====== =========== ======= ==============================================
          06/08/2018  Ting YU Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef CLS_UNIQUEID_CHECK_JOB_HPP__
#define CLS_UNIQUEID_CHECK_JOB_HPP__

#include "rtnBackgroundJobBase.hpp"
#include "monDMS.hpp"
#include "clsMgr.hpp"
using namespace std ;

namespace engine
{
   /*
    *  _clsUniqueIDCheckJob define
    */
   class _clsUniqueIDCheckJob : public _rtnBaseJob
   {
      public:
      _clsUniqueIDCheckJob () ;
      virtual ~_clsUniqueIDCheckJob () ;

   public:
      virtual RTN_JOB_TYPE type () const { return RTN_JOB_CLS_UNIQUEID_CHECK ; }

      virtual const CHAR* name () const { return "UniqueID-Check-By-Name" ; }

      virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;

      virtual INT32 doit () ;
   } ;

   typedef _clsUniqueIDCheckJob clsUniqueIDCheckJob ;

   INT32 startUniqueIDCheckJob ( EDUID* pEDUID ) ;

   /*
    *  _clsNameCheckJob define
    */
   class _clsNameCheckJob : public _rtnBaseJob
   {
   public:
      _clsNameCheckJob ( UINT64 opID ) ;
      virtual ~_clsNameCheckJob () ;

   public:
      virtual RTN_JOB_TYPE type () const
      {
         return RTN_JOB_CLS_NAME_CHECK_BY_UNIQUEID ;
      }

      virtual const CHAR* name () const { return "Name-Check-By-UniqueID" ; }

      virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;

      virtual INT32 doit () ;

   protected:
      INT32 _renameCSCL( vector<monCSSimple>& csList, BOOLEAN unregCL ) ;

      void _registerCLs( const vector<monCSSimple>& csList ) ;

      void _unregisterCL( const string& clName ) ;

      virtual void _onAttach() ;
      virtual void _onDetach() ;

   private:
      UINT64 _opID ;
      map<string, string> _mapRegisterCL ; // <failed cl name, its maincl name>
      clsFreezingWindow* _pFreezeWindow ;
      shardCB* _pShdMgr ;
      BOOLEAN  _hasBlockGlobal ;
   } ;

   typedef _clsNameCheckJob clsNameCheckJob ;

   INT32 startNameCheckJob ( EDUID* pEDUID = NULL ) ;

}

#endif

