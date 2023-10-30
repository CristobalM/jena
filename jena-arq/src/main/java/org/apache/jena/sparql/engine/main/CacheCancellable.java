package org.apache.jena.sparql.engine.main;

import org.apache.http.concurrent.Cancellable;

public class CacheCancellable implements Cancellable {

  private Cancellable cancellable = null;
  private boolean was_canceled = false;
  @Override
  public boolean cancel() {
    if(cancellable != null){
      was_canceled = true;
      return cancellable.cancel();
    }
    return false;
  }

  public void setCancellable(Cancellable cancellable){
    this.cancellable = cancellable;
  }

  public boolean isCanceled(){
    return was_canceled;
  }
}
