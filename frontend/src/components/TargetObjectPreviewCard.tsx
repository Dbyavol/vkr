import type { ReactNode } from "react";

type TargetObjectPreviewItem = {
  key: string;
  label: string;
  value: string;
};

type TargetObjectPreviewCardProps = {
  objectId: string;
  title: string;
  items: TargetObjectPreviewItem[];
  actions?: ReactNode;
};

export function TargetObjectPreviewCard({ objectId, title, items, actions }: TargetObjectPreviewCardProps) {
  if (!items.length) return null;

  return (
    <div className="target-object-preview">
      <div className="target-object-preview-head">
        <div className="target-object-preview-head-row">
          <span className="section-kicker">Выбранный объект</span>
          {actions ? <div className="target-object-preview-actions">{actions}</div> : null}
        </div>
        <strong>{title}</strong>
        <span>ID: {objectId}</span>
      </div>
      <div className="target-object-preview-grid">
        {items.map((item) => (
          <div className="target-object-preview-item" key={item.key}>
            <span>{item.label}</span>
            <strong title={item.value}>{item.value}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

export type { TargetObjectPreviewCardProps, TargetObjectPreviewItem };
