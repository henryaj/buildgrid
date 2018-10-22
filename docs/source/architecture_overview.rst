.. _architecture-overview:

Remote execution overview
=========================

Remote execution aims to speed up a build process and to rely on two separate
but related concepts that are remote caching and remote execution itself. Remote
caching allows users to share build outputs while remote execution allows the running
of operations on a remote cluster of machines which may be more powerful than what
the user has access to locally.

The `Remote Execution API`_ (REAPI) describes a `gRPC`_ + `protocol-buffers`_
interface that has three main services for remote caching and execution:

- A ``ContentAddressableStorage`` (CAS) service: a remote storage end-point
  where content is addressed by digests, a digest being a pair of the hash and
  size of the data stored or retrieved.
- An ``ActionCache`` (AC) service: a mapping between build actions already
  performed and their corresponding resulting artifact.
- An ``Execution`` service: the main end-point allowing one to request build
  job to be perform against the build farm.

The `Remote Worker API`_ (RWAPI) describes another `gRPC`_ + `protocol-buffers`_
interface that allows a central ``BotsService`` to manage a farm of pluggable workers.

BuildGrid is combining these two interfaces in order to provide a complete
remote caching and execution service. The high level architecture can be
represented like this:

.. graphviz::
   :align: center

    digraph remote_execution_overview {
	node [shape = record,
	      width=2,
	      height=1];

	ranksep = 2
	compound=true
	edge[arrowtail="vee"];
	edge[arrowhead="vee"];

	client [label = "Client",
	color="#0342af",
	fillcolor="#37c1e8",
	style=filled,
	shape=box]

	subgraph cluster_controller{
	    label = "Controller";
	    labeljust = "c";
	    fillcolor="#42edae";
	    style=filled;
	    controller [label = "{ExecutionService|BotsInterface\n}",
			fillcolor="#17e86a",
			style=filled];

	}

	subgraph cluster_worker0 {
	    label = "Worker 1";
	    labeljust = "c";
	    color="#8e7747";
	    fillcolor="#ffda8e";
	    style=filled;
	    bot0 [label = "{Bot|Host-tools}"
		  fillcolor="#ffb214",
		  style=filled];
	}

	subgraph cluster_worker1 {
	    label = "Worker 2";
	    labeljust = "c";
	    color="#8e7747";
	    fillcolor="#ffda8e";
	    style=filled;
	    bot1 [label = "{Bot|BuildBox}",
		  fillcolor="#ffb214",
		  style=filled];
	}

	client -> controller [
	    dir = "both",
	    headlabel = "REAPI",
	    labelangle = 20.0,
	    labeldistance = 9,
	    labelfontsize = 15.0,
	    lhead=cluster_controller];

	controller -> bot0 [
	    dir = "both",
	    labelangle= 340.0,
		labeldistance = 7.5,
		labelfontsize = 15.0,
	    taillabel = "RWAPI     ",
	    lhead=cluster_worker0,
	    ltail=cluster_controller];

	controller -> bot1 [
	    dir = "both",
	    labelangle= 20.0,
	    labeldistance = 7.5,
	    labelfontsize = 15.0,
		taillabel = "     RWAPI",
	    lhead=cluster_worker1,
	    ltail=cluster_controller];

    }

BuildGrid can be split up into separate endpoints. It is possible to have
a separate ``ActionCache`` and ``CAS`` from the ``Controller``. The
following diagram shows a typical setup.

.. graphviz::
   :align: center

    digraph remote_execution_overview {

	node [shape=record,
	      width=2,
	      height=1];

	compound=true
	graph [nodesep=1,
	       ranksep=2]

	edge[arrowtail="vee"];
	edge[arrowhead="vee"];

	client [label="Client",
		color="#0342af",
		fillcolor="#37c1e8",
		style=filled,
		shape=box]

	cas [label="CAS",
	     color="#840202",
	     fillcolor="#c1034c",
	     style=filled,
	     shape=box]

	subgraph cluster_controller{
	    label="Controller";
	    labeljust="c";
	    fillcolor="#42edae";
	    style=filled;
	    controller [label="{ExecutionService|BotsInterface\n}",
			fillcolor="#17e86a",
			style=filled];

	}

	actioncache [label="ActionCache",
		     color="#133f42",
		     fillcolor="#219399",
		     style=filled,
		     shape=box]

	subgraph cluster_worker0 {
	    label="Worker";
	    labeljust="c";
	    color="#8e7747";
	    fillcolor="#ffda8e";
	    style=filled;
	    bot0 [label="{Bot}"
		  fillcolor="#ffb214",
		  style=filled];
	}

	client -> controller [
	    dir="both"];

	client -> cas [
	    dir="both",
	    lhead=cluster_controller];

	controller -> bot0 [
	    dir="both",
	    lhead=cluster_worker0];
	    //ltail=cluster_controller];

	cas -> bot0 [
	    dir="both",
	    lhead=cluster_worker0];

	actioncache -> controller [
	    dir="both"];

	client -> actioncache [
	    dir="both",
	    constraint=false,
    ];


    }

.. _Remote Execution API: https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/execution/v2
.. _gRPC: https://grpc.io
.. _protocol-buffers: https://developers.google.com/protocol-buffers
.. _Remote Worker API: https://github.com/googleapis/googleapis/tree/master/google/devtools/remoteworkers/v1test2
