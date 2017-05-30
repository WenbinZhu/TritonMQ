package edu.ucsd.tritonmq.common;

import java.io.Serializable;

public interface CommonRecord<T> extends Serializable {
    String topic();
    T value();
}
