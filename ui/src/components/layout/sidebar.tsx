"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Radio,
  Network,
  FlaskConical,
  Scale,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";

interface NavItem {
  label: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  badge?: number;
}

interface SidebarProps {
  pendingDecisions?: number;
}

export function Sidebar({ pendingDecisions = 0 }: SidebarProps) {
  const pathname = usePathname();

  const navItems: NavItem[] = [
    { label: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
    { label: "Events", href: "/events", icon: Radio },
    { label: "Federation", href: "/federation", icon: Network },
    { label: "Simulator", href: "/simulator", icon: FlaskConical },
    {
      label: "Decisions",
      href: "/decisions",
      icon: Scale,
      badge: pendingDecisions > 0 ? pendingDecisions : undefined,
    },
  ];

  return (
    <aside className="flex h-full w-16 flex-col items-center border-r bg-muted/40 py-4 lg:w-56">
      <div className="mb-6 px-4 text-sm font-bold tracking-tight text-foreground lg:text-base">
        <span className="hidden lg:inline">Baran OS</span>
        <span className="lg:hidden">B</span>
      </div>

      <nav className="flex flex-1 flex-col gap-1 px-2 w-full">
        {navItems.map((item) => {
          const isActive =
            pathname === item.href || pathname.startsWith(item.href + "/");
          const Icon = item.icon;

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground",
              )}
            >
              <Icon className="h-5 w-5 shrink-0" />
              <span className="hidden lg:inline">{item.label}</span>
              {item.badge !== undefined && (
                <Badge
                  variant="destructive"
                  className="ml-auto hidden h-5 min-w-5 items-center justify-center rounded-full px-1.5 text-xs lg:flex"
                >
                  {item.badge}
                </Badge>
              )}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
