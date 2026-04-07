"use client";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { DecisionList, DecisionHistory } from "@/components/decisions/decision-list";
import { useDecisions } from "@/hooks/use-decisions";

export default function DecisionsPage() {
  const { pending, history, loading, error, respond } = useDecisions();

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Human Decisions</h1>
        <p className="text-sm text-muted-foreground">
          Approve or reject pending workflow decisions.
        </p>
      </div>

      {error && (
        <div className="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
          {error}
        </div>
      )}

      <Tabs defaultValue="pending" className="w-full">
        <TabsList>
          <TabsTrigger value="pending" className="gap-2">
            Pending
            {pending.length > 0 && (
              <Badge variant="destructive" className="text-xs">
                {pending.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="history">History</TabsTrigger>
        </TabsList>

        <TabsContent value="pending" className="mt-4">
          {loading ? (
            <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
              Loading decisions…
            </div>
          ) : (
            <DecisionList decisions={pending} onRespond={respond} />
          )}
        </TabsContent>

        <TabsContent value="history" className="mt-4">
          {loading ? (
            <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
              Loading decisions…
            </div>
          ) : (
            <DecisionHistory decisions={history} />
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
