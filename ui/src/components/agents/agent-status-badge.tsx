import { Badge } from "@/components/ui/badge";
import type { AgentStatus } from "@/lib/types";

const TONE: Record<AgentStatus, string> = {
  active: "bg-green-100 text-green-800 border-green-300",
  unhealthy: "bg-yellow-100 text-yellow-800 border-yellow-300",
  dead: "bg-red-100 text-red-800 border-red-300",
  unregistered: "bg-gray-100 text-gray-700 border-gray-300",
  unknown: "bg-gray-100 text-gray-700 border-gray-300",
};

export function AgentStatusBadge({ status }: { status: AgentStatus }) {
  return (
    <Badge variant="outline" className={TONE[status]}>
      {status}
    </Badge>
  );
}
