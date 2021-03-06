package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {


    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxPages;
    private Map<PageId, Page> pool;
    private LockManager lm;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxPages = numPages;
        // This LinkedHashMap behaives like and LRU
        this.pool = Collections.synchronizedMap(new LinkedHashMap<PageId, Page>(maxPages + 1, .75F, true) {
            public synchronized boolean removeEldestEntry(Map.Entry eldest) {
                boolean removeEldest = size() > maxPages;

                if (((Page) eldest.getValue()).isDirty() != null)
                    return false;

                if (removeEldest) {
                    // if we remove the page, flush it
                    try {
                        flushPage((PageId) eldest.getKey());
                    } catch (IOException e) {
                        System.err.println(e);
                    }
                }

                return removeEldest;
            }
        });
        this.lm = new LockManager();

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        
        // we dont need to check this anymore becuase have an eviction policy
        //if (pool.size() >= maxPages)
        //    throw new DbException("BufferPool has reached limit of " + maxPages);
        try {
            if(perm == Permissions.READ_WRITE)
                lm.writeLock(tid, pid);
            else
                lm.readLock(tid,pid);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

        Page page = pool.get(pid);

        if (page != null) {
            return page;
        }

        Catalog catalog = Database.getCatalog();
        page = catalog.getDbFile(pid.getTableId()).readPage(pid);
        if (!canEvictPage()) 
            throw new DbException("Connot evict any pages");
        pool.put(page.getId(), page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        lm.readUnlock(tid, pid);
        lm.writeUnlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return (lm.holdsReadLock(tid, p) || lm.holdsWriteLock(tid, p));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
        // take care of pages
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for(Page page : pool.values()) {
                if (page.isDirty() == tid) {
                    pool.put(page.getId(), page.getBeforeImage());
                    page.markDirty(false, tid);
                }
            }
        }

        Iterator<Page> theIterator = this.pool.values().iterator();
        while(theIterator.hasNext()) {
            Page page = theIterator.next();
            PageId pid = page.getId();
            releasePage(tid, page.getId());
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        assert tid != null;
        
        ArrayList<Page> dirtyPages = Database.getCatalog().getDbFile(tableId).insertTuple(tid, t);
        for(Page page : dirtyPages) {
            assert tid != null;
            page.markDirty(true, tid);
            assert tid.equals(page.isDirty()) == true;
            pool.put(page.getId(), page);
            assert tid.equals(pool.get(page.getId()).isDirty()) == true;
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int id = t.getRecordId().getPageId().getTableId();
        Page page = Database.getCatalog().getDbFile(id).deleteTuple(tid, t);
        assert tid != null;
        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1

        for (Page page : pool.values()) {
            Catalog catalog = Database.getCatalog();
            PageId pid = page.getId();
            DbFile pageFile = catalog.getDbFile(pid.getTableId());
            if (page.isDirty() != null) {
                assert page.isDirty() != null;
                page.markDirty(false, page.isDirty());
                pageFile.writePage(page);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        pool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        Catalog catalog = Database.getCatalog();
        DbFile pageFile = catalog.getDbFile(pid.getTableId());
        Page page = pool.get(pid);
        if (page.isDirty() != null) {
            pageFile.writePage(page);
            page.markDirty(false, page.isDirty());
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        ArrayList<Page> pages = new ArrayList<Page>();
        for(Page page : pool.values()) {
            pages.add(page);
        }
        for(int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            if(page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    private synchronized boolean canEvictPage() {
        if (pool.size() < maxPages)
            return true;

        for(Page page : pool.values()) {
            if(page.isDirty() == null)
                return true;
        }

        return false;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1

        // this method is not necessary due to my lru implementation
        //pool.put(null, null);
        //pool.remove(null);
    }

}
