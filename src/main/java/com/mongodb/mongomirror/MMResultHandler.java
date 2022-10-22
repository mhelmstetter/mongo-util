package com.mongodb.mongomirror;

import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;

public class MMResultHandler extends DefaultExecuteResultHandler {
    MMEventListener mmEventListener;

    public MMResultHandler(MMEventListener mmEventListener) {
        this.mmEventListener = mmEventListener;
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
        super.onProcessFailed(e);
        mmEventListener.procFailed(e);
    }
}
