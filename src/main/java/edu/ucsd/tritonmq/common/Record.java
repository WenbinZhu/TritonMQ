package edu.ucsd.tritonmq.common;

import java.io.Serializable;


public interface Record<T> extends Serializable {
    String topic();
    T value();
}
