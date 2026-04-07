"use client";

import { useCallback, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type NodeMouseHandler,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { useFederation, type ClusterNodeData } from "@/hooks/use-federation";
import { ClusterNode } from "./cluster-node";
import { RelayEdge } from "./relay-edge";
import { ClusterDetail } from "./cluster-detail";
import { FederationEmptyState } from "./federation-empty-state";
import type { Cluster } from "@/lib/types";

const nodeTypes = { cluster: ClusterNode };
const edgeTypes = { relay: RelayEdge };

export function FederationGraph() {
  const { clusters, nodes, edges, loading, error } = useFederation();
  const [selected, setSelected] = useState<Cluster | null>(null);

  const onNodeClick = useCallback<NodeMouseHandler<Node<ClusterNodeData>>>(
    (_e, node) => {
      setSelected(node.data.cluster);
    },
    [],
  );

  const isEmpty = !loading && !error && clusters.length === 0;

  const content = useMemo(() => {
    if (loading) {
      return (
        <div className="flex items-center justify-center h-full text-muted-foreground">
          Loading federation topology…
        </div>
      );
    }
    if (error) {
      return (
        <div className="flex items-center justify-center h-full text-destructive text-sm">
          Failed to load federation: {error}
        </div>
      );
    }
    if (isEmpty) return <FederationEmptyState />;
    return (
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={onNodeClick}
        fitView
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap pannable zoomable />
      </ReactFlow>
    );
  }, [loading, error, isEmpty, nodes, edges, onNodeClick]);

  return (
    <div className="h-full w-full">
      {content}
      <ClusterDetail cluster={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
