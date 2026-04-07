import { Network } from "lucide-react";

export function FederationEmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center p-8 gap-3">
      <Network className="w-12 h-12 text-muted-foreground" />
      <h2 className="text-lg font-medium">No federated clusters</h2>
      <p className="text-sm text-muted-foreground max-w-md">
        Federation is not configured for this Baran OS instance, or no peer
        clusters have joined yet. Configure federation in the runtime to see
        the network topology here.
      </p>
    </div>
  );
}
