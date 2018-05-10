package com.sequoiadb.datasource;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


abstract class AbstractStrategy implements IConnectStrategy {

    protected ArrayDeque<ConnItem> _activeConnItemDeque = new ArrayDeque<ConnItem>();
    protected ArrayDeque<ConnItem> _newlyCreatedConnItemDeque = new ArrayDeque<ConnItem>();
    protected ArrayList<String> _addrs = new ArrayList<String>();
    protected Lock _opLock = new ReentrantLock();
    protected Lock _addrLock = new ReentrantLock();

    @Override
    public void init(List<String> addressList, List<Pair> _idleConnPairs, List<Pair> _usedConnPairs) {

        Iterator<String> addrListItr = addressList.iterator();
        while (addrListItr.hasNext()) {
            String addr = addrListItr.next();
            _addAddress(addr);
        }
        if (_idleConnPairs != null) {
            Iterator<Pair> idleConnPairItr = _idleConnPairs.iterator();
            while (idleConnPairItr.hasNext()) {
                Pair pair = idleConnPairItr.next();
                String addr = pair.first().getAddr();
                _addConnItem(pair.first());
                _addAddress(addr);
            }
        }
    }

    @Override
    public abstract String getAddress();


    @Override
    public ConnItem pollConnItemForGetting() {
        _opLock.lock();
        try {
            ConnItem connItem = _activeConnItemDeque.pollFirst();
            if (connItem == null) {
                connItem = _newlyCreatedConnItemDeque.pollFirst();
            }
            return connItem;
        } finally {
            _opLock.unlock();
        }
    }

    @Override
    public ConnItem pollConnItemForDeleting() {
        _opLock.lock();
        try {
            ConnItem connItem = _newlyCreatedConnItemDeque.pollFirst();
            if (connItem == null) {
                connItem = _activeConnItemDeque.pollLast();
            }
            return connItem;
        } finally {
            _opLock.unlock();
        }
    }

    @Override
    public ConnItem peekConnItemForDeleting() {
        _opLock.lock();
        try {
            ConnItem connItem = _newlyCreatedConnItemDeque.peekFirst();
            if (connItem == null) {
                connItem =  _activeConnItemDeque.peekLast();
            }
            return connItem;
        } finally {
            _opLock.unlock();
        }
    }

    @Override
    public void removeConnItemAfterCleaning(ConnItem connItem) {
    }

    @Override
    public void updateUsedConnItemCount(ConnItem connItem, int change) {
    }

    @Override
    public void addAddress(String addr) {
        _addAddress(addr);
    }

    @Override
    public List<ConnItem> removeAddress(String addr) {
        List<ConnItem> returnList = new ArrayList<ConnItem>();
        _addrLock.lock();
        try {
            if (_addrs.contains(addr)) {
                _addrs.remove(addr);
            }
        } finally {
            _addrLock.unlock();
        }
        _opLock.lock();
        try {
            Iterator<ConnItem> iterator = _activeConnItemDeque.iterator();
            while (iterator.hasNext()) {
                ConnItem connItem = iterator.next();
                if (addr.equals(connItem.getAddr())) {
                	returnList.add(connItem);
                    iterator.remove();
                }
            }
            iterator = _newlyCreatedConnItemDeque.iterator();
            while (iterator.hasNext()) {
                ConnItem connItem = iterator.next();
                if (addr.equals(connItem.getAddr())) {
                    returnList.add(connItem);
                    iterator.remove();
                }
            }
        } finally {
            _opLock.unlock();
        }
        return returnList;
    }

    @Override
    public void addConnItemAfterCreating(ConnItem connItem) {
        _addConnItem(connItem);
    }

    @Override
    public void addConnItemAfterReleasing(ConnItem connItem) {
        _releaseConnItem(connItem);
    }

    private void _addConnItem(ConnItem connItem) {
        _opLock.lock();
        try {
            _newlyCreatedConnItemDeque.addLast(connItem);
        } finally {
            _opLock.unlock();
        }
    }

    private void _releaseConnItem(ConnItem connItem) {
        _opLock.lock();
        try {
            _activeConnItemDeque.addFirst(connItem);
        } finally {
            _opLock.unlock();
        }
    }

    private void _addAddress(String addr) {
        _addrLock.lock();
        try {
            if (!_addrs.contains(addr)) {
                _addrs.add(addr);
            }
        } finally {
            _addrLock.unlock();
        }
    }
}
