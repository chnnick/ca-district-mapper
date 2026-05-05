import type { ReactNode } from "react";

interface Props {
  children: ReactNode;
}

export default function Sidebar({ children }: Props) {
  return (
    <aside className="sidebar">
      <div className="sidebar-header">CA District Mapper</div>
      <div className="sidebar-body">{children}</div>
    </aside>
  );
}
