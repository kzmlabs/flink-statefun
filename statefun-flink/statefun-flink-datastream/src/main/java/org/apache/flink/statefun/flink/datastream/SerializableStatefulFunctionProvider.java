// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package org.apache.flink.statefun.flink.datastream;

import java.io.Serializable;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

/** {@inheritDoc} */
public interface SerializableStatefulFunctionProvider
    extends StatefulFunctionProvider, Serializable {}
