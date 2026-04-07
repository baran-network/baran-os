"use client";

import {
  BaseEdge,
  EdgeLabelRenderer,
  getStraightPath,
  type EdgeProps,
} from "@xyflow/react";

interface RelayEdgeData {
  active?: boolean;
  [key: string]: unknown;
}

export function RelayEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  data,
}: EdgeProps & { data?: RelayEdgeData }) {
  const [edgePath, labelX, labelY] = getStraightPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
  });

  const active = data?.active ?? false;
  const stroke = active ? "#22c55e" : "#94a3b8";

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={{ stroke, strokeWidth: 2, strokeDasharray: active ? "0" : "4 4" }}
      />
      <EdgeLabelRenderer>
        <div
          style={{
            position: "absolute",
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
          }}
          className="text-[10px] text-muted-foreground bg-background px-1 rounded pointer-events-none"
        >
          {active ? "relay" : "idle"}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}
