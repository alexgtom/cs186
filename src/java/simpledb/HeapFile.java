package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile fp;

        try {
            fp = new RandomAccessFile(f, "r");
        } catch(java.io.FileNotFoundException e) {
            throw new IllegalArgumentException();
        }

        try {
            byte[] page = new byte[BufferPool.PAGE_SIZE];
            fp.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            fp.read(page, 0, BufferPool.PAGE_SIZE);
            fp.close();
            return new HeapPage(
                new HeapPageId(pid.getTableId(), pid.pageNumber()), page);
        } catch(IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.seek(offset);
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        ArrayList<Page> pageList = new ArrayList<Page>();

        // find and empty page
        HeapPage page = null;
        for(int i = 0; i < numPages(); i++) {
             HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, 
                     new HeapPageId(getId(), i), Permissions.READ_WRITE);
             if (p.getNumEmptySlots() > 0) {
                 page = p;
                 pageList.add(page);
                 page.insertTuple(t);
                 return pageList;
             }
        }
        
        // could not find a page, create a new one
        // create heap page
        long initPages = numPages();
        HeapPageId pid = new HeapPageId(getId(), numPages());
        page = new HeapPage(pid, HeapPage.createEmptyPageData());

        FileOutputStream file = new FileOutputStream(f, true);
        file.write(page.getPageData());
        file.close();

        page = (HeapPage) Database.getBufferPool().getPage(
                tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);

        assert numPages() > initPages;

        return pageList;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        return page;
    }

    private class HeapFileIterator implements DbFileIterator {
        private int pageNo;
        private Iterator<Tuple>pageIt;
        private boolean opened;
        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        public void open()
            throws DbException, TransactionAbortedException {
            pageNo = 0;
            
            int i = findNextIteratorIndex(0);

            if (i == -1)
                pageIt = null;
            else
                pageIt = getIteratorAtIndex(i);

            opened = true;
        }

        public boolean hasNext()
            throws DbException, TransactionAbortedException {
            if (!opened)
                return false;

            if (pageIt == null)
                return false;

            // if has next, then return true
            if (pageIt.hasNext())
                return true;

            if (pageNo >= numPages())
                return false;

            // if dosent have next, then search for the next iterator
            int i = findNextIteratorIndex(pageNo + 1);
            if (i == -1)
                return false;
            else
                // reach end of pages and found no tuples
                return true;
        }

        private Iterator<Tuple> getIteratorAtIndex(int i)
            throws DbException, TransactionAbortedException, NoSuchElementException {
            HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(
                tid,
                new HeapPageId(getId(), i),
                Permissions.READ_WRITE
            );
            return currentPage.iterator();
        } 

        private int findNextIteratorIndex(int i) 
            throws DbException, TransactionAbortedException, NoSuchElementException {
            for(; i < numPages(); i++) {
                if(getIteratorAtIndex(i).hasNext())
                    return i;
            }

            return -1;
        }

        public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException {

            if (!opened || pageIt == null || pageNo >= numPages())
                throw new NoSuchElementException();

            // current iterator has tuples
            if (pageIt.hasNext())
                return pageIt.next();
            
            // current iterator has no more tuples, find next one
            pageNo++;
            int i = findNextIteratorIndex(pageNo);
            if (i == -1)
                // reach end of pages and found no tuples
                throw new NoSuchElementException("No more tuples");
            else {
                pageNo = i;
                pageIt = getIteratorAtIndex(i);
                return pageIt.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            opened = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

