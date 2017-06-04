package edu.ucsd.tritonmq.common;

import java.io.Serializable;

/**
 * Created by Wenbin on 5/31/17.
 */
public interface Record<T> extends Serializable {
    String topic();
    T value();
}
