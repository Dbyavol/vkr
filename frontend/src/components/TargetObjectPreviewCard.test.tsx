import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { TargetObjectPreviewCard } from "./TargetObjectPreviewCard";


describe("TargetObjectPreviewCard", () => {
  it("renders selected object title, id and transformed field preview", () => {
    render(
      <TargetObjectPreviewCard
        objectId="42"
        title="Москва · Хамовники"
        items={[
          { key: "price", label: "Price", value: "12 500 000" },
          { key: "area", label: "Area", value: "54.2" },
        ]}
      />,
    );

    expect(screen.getByText("Выбранный объект")).toBeInTheDocument();
    expect(screen.getByText("Москва · Хамовники")).toBeInTheDocument();
    expect(screen.getByText("ID: 42")).toBeInTheDocument();
    expect(screen.getByText("Price")).toBeInTheDocument();
    expect(screen.getByText("12 500 000")).toBeInTheDocument();
    expect(screen.getByText("Area")).toBeInTheDocument();
  });

  it("does not render empty cards", () => {
    const { container } = render(<TargetObjectPreviewCard objectId="1" title="Object 1" items={[]} />);
    expect(container).toBeEmptyDOMElement();
  });
});
