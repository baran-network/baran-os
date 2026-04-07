"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchFederationNodes } from "@/lib/api";
import type { Cluster, Event } from "@/lib/types";
import { useConnection } from "@/hooks/use-connection";
import type { Edge, Node } from "@xyflow/react";

export interface ClusterNodeData extends Record<string, unknown> {
  cluster: Cluster;
}

export interface UseFederationReturn {
  clusters: Cluster[];
  nodes: Node<ClusterNodeData>[];
  edges: Edge[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useFederation(): UseFederationReturn {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      const list = await fetchFederationNodes();
      setClusters(list);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const handleEvent = useCallback(
    (ev: Event) => {
      if (ev.type.startsWith("federation.")) {
        void refresh();
      }
    },
    [refresh],
  );

  useConnection({ onEvent: handleEvent });

  const { nodes, edges } = useMemo(() => {
    const radius = Math.max(180, clusters.length * 40);
    const nodes: Node<ClusterNodeData>[] = clusters.map((cluster, i) => {
      const angle = (i / Math.max(1, clusters.length)) * 2 * Math.PI;
      return {
        id: cluster.id,
        type: "cluster",
        position: {
          x: Math.cos(angle) * radius + radius + 50,
          y: Math.sin(angle) * radius + radius + 50,
        },
        data: { cluster },
      };
    });

    // Build a fully-connected mesh of relay edges between known nodes.
    const edges: Edge[] = [];
    for (let i = 0; i < clusters.length; i++) {
      for (let j = i + 1; j < clusters.length; j++) {
        const a = clusters[i];
        const b = clusters[j];
        const active =
          a.status === "active" && b.status === "active";
        edges.push({
          id: `${a.id}-${b.id}`,
          source: a.id,
          target: b.id,
          type: "relay",
          data: { active },
        });
      }
    }

    return { nodes, edges };
  }, [clusters]);

  return { clusters, nodes, edges, loading, error, refresh };
}
