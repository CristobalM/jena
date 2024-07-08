package org.apache.jena.tdb.solver;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorConcat;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.main.CacheCancellable;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChainSelecter implements Iterator<BindingNodeId> {


  public enum Selection {
    NOT_YET,
    JENA,
    CACHE
  }
  Thread jenaThread;
  Thread cacheThread;

  private boolean startedSelecting;


  Iterator<BindingNodeId> cacheIt;
  Iterator<BindingNodeId> jenaIt;
  List<Abortable> killListCache;
  List<Abortable> killListJena;
  CacheCancellable cacheCancellable;

  public ChainSelecter(
    Iterator<BindingNodeId> cacheIt,
    Iterator<BindingNodeId> jenaIt,
    List<Abortable> killListCache,
    List<Abortable> killListJena,
    CacheCancellable cacheCancellable){
    this.cacheIt = cacheIt;
    this.jenaIt = jenaIt;
    this.killListCache = killListCache;
    this.killListJena = killListJena;
    this.startedSelecting = false;
    this.cacheCancellable = cacheCancellable;
  }

  @Override
  public boolean hasNext() {
    startSelecting();
    return status.selectedIt.hasNext();
  }

  @Override
  public BindingNodeId next() {
    return status.selectedIt.next();
  }


  private void startSelecting(){
    if (startedSelecting) {
      return;
    }
    startedSelecting = true;
    ThreadStarter jenaStarter = new ThreadStarter(jenaIt, status, Selection.JENA);
    ThreadStarter cacheStarter = new ThreadStarter(cacheIt, status, Selection.CACHE);
    jenaThread = new Thread(jenaStarter);
    cacheThread = new Thread(cacheStarter);
    jenaThread.start();
    cacheThread.start();
    status.lock.lock();
    long elapsedTime;
    try {
      while (status.selected == Selection.NOT_YET) {
        try {
          status.condVar.await();
        } catch (InterruptedException e) {
          System.out.println("Interrupted exception" + e);
        }
      }

      if (status.selected == Selection.JENA) {
        if(cacheCancellable != null){
          cacheCancellable.cancel();
        }
        killListCache.forEach(Abortable::abort);
        elapsedTime = jenaStarter.getTimeElapsed();
        System.out.println("Jena was faster, took " + elapsedTime);
      } else {
        killListJena.forEach(Abortable::abort);
        elapsedTime = cacheStarter.getTimeElapsed();
        System.out.println("Cache was faster, took " + elapsedTime);
      }
    } finally {
      status.lock.unlock();
    }
    try{
      jenaThread.join();
    }catch (InterruptedException e) {
      throw new RuntimeException("failed to join jena thread" + e);
    }
    try{
      cacheThread.join();
    }catch (InterruptedException e) {
      throw new RuntimeException("failed to join cache thread" + e);
    }
  }

  public class ExecStatus {
    public Lock lock = new ReentrantLock();
    public Condition condVar = lock.newCondition();
    public Selection selected = Selection.NOT_YET;
    Iterator<BindingNodeId> selectedIt = null;
  }
  public final ExecStatus status = new ExecStatus();



  private class ThreadStarter implements Runnable {

    private final Iterator<BindingNodeId> givenIt;
    private long timeElapsed;
    private ExecStatus status;
    private Selection selection;

    public ThreadStarter(
      Iterator<BindingNodeId> givenIt,
      ExecStatus status,
      Selection selection) {
      this.givenIt = givenIt;
      this.status = status;
      this.timeElapsed = -1;
      this.selection = selection;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      BindingNodeId firstResult = null;
      if (givenIt.hasNext()) {
        firstResult = givenIt.next();
      }
      timeElapsed = System.currentTimeMillis() - start;
      try {
        status.lock.lock();
        if (status.selected == Selection.NOT_YET) {
          status.selected = selection;
          if(firstResult != null){
            status.selectedIt = IteratorConcat.concat(Iter.of(firstResult), givenIt);
          }
          else {
            status.selectedIt = Iter.empty();
          }
        }
      } finally {
        status.condVar.signalAll();
        status.lock.unlock();
      }
    }

    public long getTimeElapsed() {
      return timeElapsed;
    }
  }
}
