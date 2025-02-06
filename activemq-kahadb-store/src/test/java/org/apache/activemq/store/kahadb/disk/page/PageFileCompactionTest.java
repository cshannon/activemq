package org.apache.activemq.store.kahadb.disk.page;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PageFileCompactionTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // min .1
  // max .3
  @Test
  public void compactionDefault() throws IOException {
    PageFile pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setEnableCompaction(true);
    pf.load();

    writePages(pf, 100, true);

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    freePages(pf, 0, 50, false);

    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    freePages(pf, 50, 100, false);
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.flush();
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(10), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());

  }

  @Test
  public void testCompactionUnCleanShutdown() throws IOException, InterruptedException {
    var tmp = tempFolder.newFolder();
    PageFile pf = new PageFile(tmp, "pagefile");
    pf.setEnableCompaction(true);
    pf.load();

    writePages(pf, 100, true);
    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    freePages(pf, 70, 100, true);

    assertEquals(30, pf.getFreePageCount());
    assertEquals(100, pf.getPageCount());
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(80), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());
    assertEquals(80, pf.getPageCount());



    //recovery file contains the last batch of writes if enabled, so
    // in this case the last thing written was to free 30 pages so
    // loading will bring back free pages without clean shutdown
    PageFile pf2 = new PageFile(tmp, "pagefile");
    //pf2.setEnableCompaction(true);
    pf2.load();
    // free page recovery is async
    Thread.sleep(1000);
    // flush merges recovered free pages
    pf2.flush();
    //pf2.compact();

    // restored back to before compaction
    assertEquals(pf2.getFile().length(), pf2.getDiskSize());
    assertEquals(pf2.toOffset(100), pf2.getDiskSize());
    assertEquals(30, pf2.getFreePageCount());
    assertEquals(100, pf2.getPageCount());

    // TODO: test with recovery file off, free pages should not come
    // back as they would no longer be redone on recovery
    //
    // TODO we also want to test with recovery file still enabled, but
    // writing something after compaction and then unclean shutdown
    // In that case the recovery file would contain the new writes
    // and not the free pages so the compaction should hopefully hold
    // Ie if we compacted from 30 to 10 free pages, then write 1 page
    // and unclean shutdown, we would recover with 9 free pages left
    // as it would replay the 1 new write and not recover the previous
    // free pages.

//    assertEquals(pf2.getFile().length(), pf2.getDiskSize());
//    assertEquals(pf2.toOffset(80), pf2.getDiskSize());
//    assertEquals(10, pf2.getFreePageCount());
//    assertEquals(80, pf2.getPageCount());

  }

  @Test
  public void testCompaction() throws IOException {
    PageFile pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setEnableCompaction(true);
    pf.load();

    writePages(pf, 100, true);

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    freePages(pf, 71, 100, true);
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(100), pf.getDiskSize());
    assertEquals(29, pf.getFreePageCount());
    assertEquals(100, pf.getPageCount());

    freePages(pf, 70, 71, true);
    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(80), pf.getDiskSize());
    assertEquals(10, pf.getFreePageCount());
    assertEquals(80, pf.getPageCount());

  }

  @Test
  public void testCompactionNoMinMax() throws IOException {
    PageFile pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.setEnableCompaction(true);
    pf.setMinFreePageCompactionRatio(0);
    pf.setMaxFreePageCompactionRatio(0);
    pf.load();

    allocateFree(pf, 100, true);

    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.compact();

    assertEquals(pf.getFile().length(), pf.getDiskSize());
    assertEquals(pf.toOffset(0), pf.getDiskSize());

  }

  @Test
  public void testCompactionNever() throws IOException {
    PageFile pf = new PageFile(tempFolder.newFolder(), "pagefile");
    pf.load();

    allocateFree(pf, 100, true);

    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    pf.compact();

    // compaction is disabled, should not compact
    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());

    // enable but set min/max to 100 so will never compact
    pf.unload();
    pf.setEnableCompaction(true);
    pf.setMinFreePageCompactionRatio(100);
    pf.setMaxFreePageCompactionRatio(100);
    pf.load();
    pf.compact();

    assertEquals(100, pf.getFreePageCount());
    assertEquals(pf.toOffset(100), pf.getDiskSize());
  }


  private void allocateFree(PageFile pf, int pageCount, boolean flush) throws IOException {
    // Insert some data into the page file.
    Transaction tx = pf.tx();
    for (int i = 0; i < pageCount; i++) {
      Page<String> page = tx.allocate();
      assertEquals(Page.PAGE_FREE_TYPE, page.getType());
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }

  private void writePages(PageFile pf, int pageCount, boolean flush) throws IOException {
    // Insert some data into the page file.
    Transaction tx = pf.tx();
    for (int i = 0; i < pageCount; i++) {
      Page<String> page = tx.allocate();
      assertEquals(Page.PAGE_FREE_TYPE, page.getType());

      String t = "page:" + i;
      page.set(t);
      tx.store(page, StringMarshaller.INSTANCE, false);
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }

  private void freePages(PageFile pf, int start, int end, boolean flush) throws IOException {
    Transaction tx = pf.tx();
    for (int i = start; i < end; i++) {
      tx.free(i);
    }
    tx.commit();
    if (flush) {
      pf.flush();
    }
  }
}
