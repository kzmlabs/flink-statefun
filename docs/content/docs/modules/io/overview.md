---
title: 'Overview'
weight: 1
type: docs
aliases:
  - /modules/io/
permalink: /moduless/io/index.html
---
<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2014 The Apache Software Foundation -->

# I/O Components 

Stateful Functions' I/O components allow functions to receive and send messages to external systems.
Based on the concept of Ingress (input) and Egress (output) points, and built on top of the Apache Flink® connector ecosystem, I/O components enable functions to interact with the outside world through the style of message passing.

Commonly used I/O components are bundled into the runtime by default and can be configured directly via the applications [module configuration]({{< ref "docs/modules/overview" >}}). 
Additionally, custom connectors for other systems can be [plugged in]({{< ref "docs/modules/io/flink-connectors" >}}) to the runtime.

Remember, to use one of these connectors in an application, third-party components are usually required, e.g., servers for the data stores or message queues.
