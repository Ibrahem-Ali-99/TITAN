import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { NAV_ITEMS } from "@/lib/navigation";

export function PlannedModule({ href }: { href: string }) {
  const item = NAV_ITEMS.find((entry) => entry.href === href);
  if (!item) {
    return null;
  }
  const Icon = item.icon;

  return (
    <div className="mx-auto max-w-3xl">
      <div className="mb-6 flex items-center gap-3">
        <Icon className="size-5 text-zinc-400" aria-hidden />
        <h1 className="text-2xl font-semibold tracking-tight">{item.label}</h1>
        <Badge className="border-zinc-700 text-zinc-400">Not available</Badge>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Planned for a later milestone</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm leading-6 text-zinc-300">
          <p>{item.summary}</p>
          <p>No statistics are shown because this module does not collect any.</p>
        </CardContent>
      </Card>
    </div>
  );
}
