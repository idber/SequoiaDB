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

   Source File Name = rtnContextMainCL.hpp

   Descriptive Name = RunTime MainCL Context Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Runtime component. This file contains structure for Runtime
   Context.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          5/26/2017   David Li  Split from rtnContext.hpp

   Last Changed =

*******************************************************************************/
#ifndef RTN_CONTEXT_MAIN_CL_HPP_
#define RTN_CONTEXT_MAIN_CL_HPP_

#include "rtnContext.hpp"
#include "rtnQueryOptions.hpp"
#include "rtnContextMain.hpp"
#include "rtnContextExplain.hpp"
#include "ossMemPool.hpp"

namespace engine
{
   /*
      _rtnSubCLContext
   */
   class _rtnSubCLContext : public _rtnSubContext
   {
   public:
      _rtnSubCLContext( const BSONObj &orderBy,
                        _ixmIndexKeyGen *keyGen,
                        INT64 contextID );
      virtual ~_rtnSubCLContext() ;

   public:
      const CHAR *front() ;
      INT32 pop() ;
      INT32 popN( INT32 num ) ;
      INT32 popAll() ;
      INT32 recordNum() ;
      INT32 remainLength() ;
      INT32 truncate ( INT32 num ) ;
      INT32 getOrderKey( rtnOrderKey &orderKey );
      rtnContextBuf buffer() ;
      void setBuffer( rtnContextBuf &buffer ) ;

      OSS_INLINE INT64 getDataID () const
      {
         return _contextID ;
      }

   private:
      rtnContextBuf        _buffer ;
      // Why do we need this _startPos?
      // Because of the difference between the truncation of this context and
      // the rtnContextBuf we use above.
      // The context buffer is read only(except the truncation operation). So
      // when pop of this context is called, the buffer dose not actually pop
      // out the objects. It just move its iterator forward. As for truncation,
      // the buffer will always count from the BEGINNING(including those who
      // have been popped). But this is not the case in this context. So we
      // handle this difference by using this member, to make truncation working
      // properly.
      INT32                _startPos ;
      INT32                _remainNum ;
   };
   typedef class _rtnSubCLContext rtnSubCLContext ;

   /*
      _rtnContextMainCL define
   */
   class _rtnContextMainCL : public _rtnContextMain
   {
      typedef ossPoolMap< INT64, _rtnSubCLContext*>    SUBCL_CTX_MAP ;
      DECLARE_RTN_CTX_AUTO_REGISTER()
   public:
      _rtnContextMainCL( INT64 contextID, UINT64 eduID ) ;
      ~_rtnContextMainCL();
      virtual std::string      name() const ;
      virtual RTN_CONTEXT_TYPE getType () const;
      virtual _dmsStorageUnit* getSU () { return NULL ; }

      INT32 open( const bson::BSONObj & orderBy,
                  INT64 numToReturn,
                  INT64 numToSkip ) ;

      INT32 open( const rtnQueryOptions &options,
                  const std::vector<string> &subs,
                  BOOLEAN shardSort,
                  _pmdEDUCB *cb ) ;

      INT32 addSubContext( SINT64 contextID );

      BOOLEAN requireOrder () const
      {
         return !_options.isOrderByEmpty() && !_includeShardingOrder ;
      }

      virtual BOOLEAN          isWrite() const { return _isWrite ; }
      virtual BOOLEAN          needRollback() const { return _isWrite ; }

   protected:
      virtual void _toString( stringstream &ss ) ;
      INT32   _prepareAllSubCtxDataByOrder( _pmdEDUCB *cb ) ;
      INT32   _saveEmptyOrderedSubCtx( rtnSubContext* subCtx ) ;
      INT32   _getNonEmptyNormalSubCtx( _pmdEDUCB *cb, rtnSubContext*& subCtx ) ;
      INT32   _saveEmptyNormalSubCtx( rtnSubContext* subCtx ) ;
      INT32   _saveNonEmptyNormalSubCtx( rtnSubContext* subCtx ) ;

      void    _deleteSubContexts () ;

      BOOLEAN _requireExplicitSorting () const
      {
         // 1. order is required
         //    1. sort is not empty
         //    2. sort keys are not included in sharding keys
         // 2. has more than one sub-collections
         return ( _orderedContextMap.size() > 0 || _subContextMap.size() > 1 ) &&
                requireOrder() ;
      }

   private:
      INT32 _prepareSubCLData( SINT64 contextID,
                                _pmdEDUCB * cb,
                                INT32 maxNumToReturn = -1 );

      INT32 _initSubCLContext( _pmdEDUCB *cb ) ;

      INT32 _getNextContext( _pmdEDUCB *cb,
                             INT64 &contextID ) ;

      INT32 _initArgs( const _rtnQueryOptions &options,
                       const std::vector<string> &subs,
                       BOOLEAN shardSort ) ;

   private:
      SUBCL_CTX_MAP              _subContextMap ;
      BOOLEAN                    _includeShardingOrder ;
      ossPoolList< std::string > _subs ;
      BOOLEAN                    _isWrite ;
   };
   typedef class _rtnContextMainCL rtnContextMainCL;

   /*
      _rtnContextMainCLExplain define
    */
   class _rtnContextMainCLExplain : public _rtnContextBase,
                                    public _rtnExplainMainBase
   {
      DECLARE_RTN_CTX_AUTO_REGISTER()

      public :
         _rtnContextMainCLExplain ( INT64 contextID, UINT64 eduID ) ;

         virtual ~_rtnContextMainCLExplain () ;

         string name () const ;
         RTN_CONTEXT_TYPE getType () const ;
         _dmsStorageUnit* getSU () { return NULL ; }

         INT32 open ( const rtnQueryOptions & options,
                      const std::vector<string> & subs,
                      BOOLEAN shardSort,
                      pmdEDUCB * cb ) ;

      protected :
         OSS_INLINE BOOLEAN _needReturnDataInRun () const
         {
            return TRUE ;
         }

         OSS_INLINE BOOLEAN _needResetTotalRecords () const
         {
            return TRUE ;
         }

         OSS_INLINE BOOLEAN _needParallelProcess () const
         {
            return ( !_queryOptions.isOrderByEmpty() && !_shardSort ) ;
         }

         INT32 _prepareData ( pmdEDUCB * cb ) ;

         INT32 _openSubContext ( rtnQueryOptions & options,
                                 pmdEDUCB * cb,
                                 rtnContext ** ppContext ) ;

         INT32 _prepareExplainPath ( rtnContext * context,
                                     pmdEDUCB * cb ) ;

         virtual INT32 _parseLocationOption ( const BSONObj & explainOptions,
                                              BOOLEAN & hasOption ) ;

         virtual BOOLEAN _needChildExplain ( INT64 dataID,
                                             const BSONObj & childExplain ) ;

         INT32 _buildSimpleExplain ( rtnContext * explainContext,
                                     BOOLEAN & hasMore ) ;

         virtual optExplainMergePathBase* getExplainMergePath()
         {
            return &_explainMergePath ;
         }

      protected :
         std::vector< std::string > _subCollections ;
         std::set< std::string >    _subCollectionFilter ;
         BOOLEAN                    _shardSort ;
         optExplainMergePath        _explainMergePath ;
   } ;

   typedef class _rtnContextMainCLExplain rtnContextMainCLExplain ;
}

#endif /* RTN_CONTEXT_MAIN_CL_HPP_ */
