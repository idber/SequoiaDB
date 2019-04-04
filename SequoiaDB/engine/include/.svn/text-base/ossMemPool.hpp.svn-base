/*******************************************************************************


   Copyright (C) 2011-2019 SequoiaDB Ltd.

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

   Source File Name = ossMem.hpp

   Descriptive Name = Operating System Services Memory Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains declares for all memory
   allocation/free operations.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/09/2019  CW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OSSMEMPOOL_HPP_
#define OSSMEMPOOL_HPP_
#include <boost/pool/pool_alloc.hpp>
#include <map>
#include <set>
#include <list>

/*
 * Memory pool ideal for allocation of objects one chunk at a time, such as
 * std::list, std::map. Not optimal for contiuous allocation such as std::vector
 */
template<typename T>
struct ossPoolAllocator {
    typedef boost::fast_pool_allocator<T> Type;
};

/*
 * Containers that utilize the pool allocator. Currently only has map/list/set,
 * as these are the most commonly used containers. Will add other containers in
 * the future.
 * When C++11 is supported, the following structures should be modified to use
 * alias declaration instead.
 */

/*
 * Map utilizing memory pool
 */
template < typename K, typename V, class Compare = std::less<K> >
class ossPoolMap : public std::map<K, V, Compare, boost::fast_pool_allocator<std::pair<const K, V> > >{
  /**
   * DO NOT ADD ANY MEMBER/FUNCTION IN THIS CLASS
   * DO NOT USE THIS CLASS IN POLYMORPHISM
   */
};

/*
 * Set utilizing memory pool
 */
template < typename K, class Compare = std::less<K> >
class ossPoolSet : public std::set<K, Compare, boost::fast_pool_allocator<K> >{
  /**
   * DO NOT ADD ANY MEMBER/FUNCTION IN THIS CLASS
   * DO NOT USE THIS CLASS IN POLYMORPHISM
   */
};

/*
 * List utilizing memory pool
 */
template < typename K >
class ossPoolList : public std::list<K, boost::fast_pool_allocator<K> > {
  /**
   * DO NOT ADD ANY MEMBER/FUNCTION IN THIS CLASS
   * DO NOT USE THIS CLASS IN POLYMORPHISM
   */
};
#endif


