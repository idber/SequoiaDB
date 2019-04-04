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

   Source File Name = coordRemoteSession.hpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/03/2017  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef COORD_REMOTE_SESSION_HPP__
#define COORD_REMOTE_SESSION_HPP__

#include "coordResource.hpp"
#include "pmdRemoteSession.hpp"
#include "coordRemoteHandle.hpp"
#include "rtnSessionProperty.hpp"
#include "ossMemPool.hpp"

using namespace bson ;

namespace engine
{

   class _coordSessionPropMgr ;
   class _coordGroupSel ;
   class _coordGroupSessionCtrl ;

   /*
      _IGroupSessionHandler define
   */
   class _IGroupSessionHandler
   {
      public:
         _IGroupSessionHandler() {}
         virtual ~_IGroupSessionHandler() {}

      public:
         virtual void   prepareForSend( pmdSubSession *pSub,
                                        _coordGroupSel *pSel,
                                        _coordGroupSessionCtrl *pCtrl ) = 0 ;
   } ;
   typedef _IGroupSessionHandler IGroupSessionHandler ;

   /*
      _coordSessionPropSite define
   */
   class _coordSessionPropSite : public _rtnSessionProperty
   {
      public:
         typedef ossPoolMap< UINT32, UINT64 >      MAP_GROUP_2_NODE ;
         typedef MAP_GROUP_2_NODE::iterator        MAP_GROUP_2_NODE_IT ;

         typedef ossPoolMap< UINT32, MsgRouteID >  MAP_TRANS_NODES ;
         typedef MAP_TRANS_NODES::iterator         MAP_TRANS_NODES_IT ;
         typedef MAP_TRANS_NODES::const_iterator   MAP_TRANS_NODES_CIT ;

         friend class _coordSessionPropMgr ;

      public:
         _coordSessionPropSite() ;
         virtual ~_coordSessionPropSite() ;

         void        clear() ;

         void        addLastNode( UINT32 groupID, UINT64 nodeID ) ;
         UINT64      getLastNode( UINT32 groupID ) const ;
         BOOLEAN     existNode( UINT32 groupID ) const ;
         void        delLastNode( UINT32 groupID ) ;
         void        delLastNode( UINT32 groupID, UINT64 nodeID ) ;

         /*
            Trans info
         */
         BOOLEAN     isTransNode( const MsgRouteID &routeID ) const ;
         BOOLEAN     getTransNodeRouteID( UINT32 groupID,
                                          MsgRouteID &routeID ) const ;
         BOOLEAN     hasTransNode( UINT32 groupID ) const ;
         BOOLEAN     isTransNodeEmpty() const ;

         BOOLEAN     delTransNode( const MsgRouteID &routeID ) ;
         void        addTransNode( const MsgRouteID &routeID ) ;

         UINT32      getTransNodeSize() const ;
         UINT32      dumpTransNode( SET_ROUTEID &setID ) const ;

         const MAP_TRANS_NODES* getTransNodeMap() const ;

         INT32       beginTrans( _pmdEDUCB *cb ) ;
         void        endTrans( _pmdEDUCB *cb ) ;

         _pmdEDUCB*  getEDUCB() { return _pEDUCB ; }
         pmdRemoteSessionSite* getSite() { return _pSite ; }

      protected :
         virtual void _onSetInstance () ;

      private:
         void        setEduCB( _pmdEDUCB *cb ) ;
         void        setSite( pmdRemoteSessionSite *pSite ) ;

      private:
         MAP_GROUP_2_NODE     _mapLastNodes ;
         _pmdEDUCB            *_pEDUCB ;
         pmdRemoteSessionSite *_pSite ;

         MAP_TRANS_NODES      _mapTransNodes ;
         INT32                _transResult ;
   } ;

   typedef _coordSessionPropSite coordSessionPropSite ;

   /*
      _coordSessionPropMgr define
   */
   class _coordSessionPropMgr : public _IRemoteMgrHandle,
                                public _rtnSessionProperty
   {
      typedef ossPoolMap< UINT32, coordSessionPropSite >    MAP_TID_2_PROP ;
      typedef MAP_TID_2_PROP::iterator                      MAP_TID_2_PROP_IT ;

      public:
         _coordSessionPropMgr() ;
         ~_coordSessionPropMgr() ;

         coordSessionPropSite * getSite( _pmdEDUCB *cb ) ;

      protected:
         virtual void   onRegister( _pmdRemoteSessionSite *pSite,
                                    _pmdEDUCB *cb ) ;
         virtual void   onUnreg( _pmdRemoteSessionSite *pSite,
                                 _pmdEDUCB *cb ) ;

      private:
         MAP_TID_2_PROP _mapProps ;
   } ;

   typedef _coordSessionPropMgr coordSessionPropMgr ;

   /*
      _coordGroupSel define
   */
   class _coordGroupSel : public SDBObject
   {
      public:
         _coordGroupSel() ;
         ~_coordGroupSel() ;

         void     init( coordResource *pResource,
                        coordSessionPropSite *pPropSite,
                        BOOLEAN primary = FALSE,
                        MSG_ROUTE_SERVICE_TYPE svcType =
                        MSG_ROUTE_SHARD_SERVCIE ) ;

         void     setPrimary( BOOLEAN primary ) ;
         void     setServiceType( MSG_ROUTE_SERVICE_TYPE svcType ) ;

         BOOLEAN  isPrimary() const ;
         BOOLEAN  isPreferedPrimary() const ;

         INT32    selBegin( UINT32 groupID, MsgRouteID &nodeID ) ;
         INT32    selNext( MsgRouteID &nodeID ) ;
         void     selDone() ;

         void     updateStat( const MsgRouteID &nodeID, INT32 rc ) ;

         BOOLEAN  getGroupPtrFromMap( UINT32 groupID,
                                      CoordGroupInfoPtr &groupPtr ) ;

         void     addGroupPtr2Map( CoordGroupInfoPtr &groupPtr ) ;

      protected:
         INT32    _selPrimaryBegin( MsgRouteID &nodeID ) ;
         INT32    _selOtherBegin( MsgRouteID &nodeID ) ;

      protected :
         typedef _utilArray< UINT8, CLS_REPLSET_MAX_NODE_SIZE >   COORD_POS_ARRAY ;
         typedef ossPoolList< UINT8>                              COORD_POS_LIST ;
      private:
         INT32    _calcBeginPos( clsGroupItem *pGroupItem,
                                 const rtnInstanceOption & instanceOption,
                                 UINT32 random ) ;
         INT32    _nextPos( CoordGroupInfoPtr &groupPtr,
                            const rtnInstanceOption & instanceOption,
                            UINT32 &selTimes,
                            INT32 &pos,
                            MsgRouteID &nodeID ) ;
         void     _resetStatus() ;
         void     _selectPositions ( const VEC_NODE_INFO & groupNodes,
                                     UINT32 primaryPos,
                                     const rtnInstanceOption & instanceOption,
                                     UINT32 random,
                                     COORD_POS_LIST & selectedPositions ) ;
         void     _shufflePositions ( COORD_POS_ARRAY & positionArray,
                                      COORD_POS_LIST & positionList ) ;

      private:
         coordResource           *_pResource ;
         coordSessionPropSite    *_pPropSite ;
         BOOLEAN                 _primary ;
         MSG_ROUTE_SERVICE_TYPE  _svcType ;

         CoordGroupInfoPtr       _groupPtr ;
         BOOLEAN                 _hasUpdate ;
         COORD_POS_LIST          _selectedPositions ;
         INT32                   _pos ;
         UINT32                  _selTimes ;
         UINT32                  _ignoredNum ;
         MsgRouteID              _lastNodeID ;

         CoordGroupMap           _mapGroupPtr ;
   } ;
   typedef _coordGroupSel coordGroupSel ;

   /*
      _coordCataSel define
   */
   class _coordCataSel : public SDBObject
   {
      public:
         _coordCataSel() ;
         ~_coordCataSel() ;

         INT32    bind( coordResource *pResource,
                        const CHAR *pCollectionName,
                        _pmdEDUCB *cb,
                        BOOLEAN forceUpdate = FALSE,
                        BOOLEAN isRoot = FALSE ) ;

         INT32    bind( coordResource *pResource,
                        const CoordCataInfoPtr &cataPtr,
                        BOOLEAN hasUpdated = FALSE ) ;

         void     clear() ;
         void     setUpdated( BOOLEAN updated ) ;
         BOOLEAN  hasUpdated() const ;

         CoordCataInfoPtr&  getCataPtr() ;

         INT32    getGroupLst( _pmdEDUCB *cb,
                               const CoordGroupList &exceptGrpLst,
                               CoordGroupList &groupLst,
                               const BSONObj *pQuery = NULL ) ;

         /*
            This function only used when the collection is table-partitioned,
            and valid when called after getGroupLst
         */
         CoordGroupSubCLMap& getGroup2SubsMap() ;

         INT32    updateCataInfo( const CHAR *pCollectionName,
                                  _pmdEDUCB *cb,
                                  BOOLEAN isRoot = FALSE ) ;

      private:
         coordResource        *_pResource ;
         BOOLEAN              _hasUpdate ;
         CoordCataInfoPtr     _cataPtr ;
         CoordGroupSubCLMap   _mapGrp2subs ;

   } ;
   typedef _coordCataSel coordCataSel ;

   /*
      _coordGroupSessionCtrl define
   */
   class _coordGroupSessionCtrl : public SDBObject
   {
      public:
         _coordGroupSessionCtrl() ;
         ~_coordGroupSessionCtrl() ;

         void        init( coordResource *pResource,
                           coordSessionPropSite *pPropSite,
                           coordGroupSel *pGroupSel,
                           IRemoteSessionHandler *pRemoteHandle ) ;

         BOOLEAN     canRetry( INT32 flag,
                               const MsgRouteID &nodeID,
                               UINT32 newPrimaryID,
                               BOOLEAN isReadCmd,
                               BOOLEAN canUpdate = TRUE ) ;

         BOOLEAN     canRetry( INT32 flag,
                               coordCataSel &cataSel,
                               BOOLEAN canUpdate = TRUE ) ;

         void        incRetry() ;
         void        resetRetry() ;
         UINT32      getRetryTimes() const ;

         void        setMaxRetryTimes( UINT32 maxRetryTimes ) ;

      private:
         BOOLEAN     _canRetry() const ;

      private:
         UINT32                  _retryTimes ;
         UINT32                  _maxRetryTimes ;
         coordResource           *_pResource ;
         coordSessionPropSite    *_pPropSite ;
         coordGroupSel           *_pGroupSel ;
         IRemoteSessionHandler   *_pRemoteHandle ;

   } ;
   typedef _coordGroupSessionCtrl coordGroupSessionCtrl ;

   /*
      _coordGroupSession define
   */
   class _coordGroupSession : public SDBObject
   {
      public:
         _coordGroupSession() ;
         ~_coordGroupSession() ;

         INT64             getTimeout() const ;

         /*
            timeout : 0 , will use the session's timeout attribute
                      otherwise, use the specified timeout value
         */
         INT32             init( coordResource *pResource,
                                 _pmdEDUCB *cb,
                                 INT64 timeout = 0,
                                 IRemoteSessionHandler *pHandle = NULL,
                                 IGroupSessionHandler *pGroupHandle = NULL ) ;

         void              release() ;

         void              clear() ;
         void              resetSubSession() ;

         INT32             sendMsg( MsgHeader *pSrcMsg,
                                    UINT32 groupID,
                                    const netIOVec *pIov = NULL,
                                    pmdSubSession **ppSub = NULL ) ;

         INT32             sendMsg( MsgHeader *pSrcMsg,
                                    CoordGroupList &grpLst,
                                    const netIOVec *pIov = NULL ) ;

         INT32             sendMsg( MsgHeader *pSrcMsg,
                                    CoordGroupList &grpLst,
                                    const GROUP_2_IOVEC &iov ) ;

         pmdRemoteSession*       getSession() ;
         coordGroupSel*          getGroupSel() ;
         coordSessionPropSite*   getPropSite() ;
         coordGroupSessionCtrl*  getGroupCtrl() ;
         coordRemoteHandlerBase* getBaseHandle() ;

      protected:

         INT32             _sendMsg( MsgHeader *pSrcMsg,
                                     UINT32 groupID,
                                     const netIOVec *pIov,
                                     pmdSubSession **ppSub = NULL ) ;

      private:
         pmdRemoteSessionSite          *_pSite ;
         coordSessionPropSite          *_pPropSite ;
         pmdRemoteSession              *_pSession ;
         coordGroupSel                 _groupSel ;
         coordGroupSessionCtrl         _groupCtrl ;
         coordRemoteHandlerBase        _baseHandle ;

         IGroupSessionHandler          *_pGroupHandle ;

         INT64                         _timeout ;

   } ;
   typedef _coordGroupSession coordGroupSession ;

}

#endif // COORD_REMOTE_SESSION_HPP__

