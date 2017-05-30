package edu.ucsd.tritonmq.common;

import java.util.UUID;

/**
 * Created by dangyi on 5/29/17.
 */
public interface Callback {
    void onCompletion(UUID uuid, Exception exception);
}
