"use client";

import * as React from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Slider } from "@/components/ui/slider";
import { Checkbox } from "@/components/ui/checkbox";
import { Separator } from "@/components/ui/separator";
import { MultiSelect } from "@/components/MultiSelect";
import type { FilterParams } from "@/lib/filters";

export interface FilterOptions {
  deltaPoints: number[];
  stopLossValues: number[];
  assets: string[];
  parameterSets: {
    id: number;
    name: string;
    delta: number;
    s0: number;
    stopLoss: number | null;
  }[];
  priceRegimes: string[];
  timeRemainingBuckets: string[];
  combinedSpreadBuckets: string[];
  daysOfWeek: { value: number; label: string }[];
}

interface FilterSidebarProps {
  options: FilterOptions;
  filters: FilterParams;
  onChange: (filters: FilterParams) => void;
  onApply: () => void;
  onReset: () => void;
  loading?: boolean;
}

export function FilterSidebar({
  options,
  filters,
  onChange,
  onApply,
  onReset,
  loading,
}: FilterSidebarProps) {
  const update = (partial: Partial<FilterParams>) => {
    onChange({ ...filters, ...partial });
  };

  const hourRange = filters.hourRange || [0, 23];

  return (
    <div className="flex flex-col gap-4 p-4 text-sm">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Filters</h2>
        <Button variant="ghost" size="sm" onClick={onReset}>
          Reset
        </Button>
      </div>

      <Separator />

      {/* Delta */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Delta (points)
        </Label>
        <MultiSelect
          options={options.deltaPoints.map((d) => ({
            value: String(d),
            label: String(d),
          }))}
          selected={(filters.deltaPoints || []).map(String)}
          onChange={(vals) => update({ deltaPoints: vals.map(Number) })}
          placeholder="All deltas"
        />
      </div>

      {/* Stop Loss */}
      {options.stopLossValues.length > 0 && (
        <div className="space-y-1.5">
          <Label className="text-xs font-medium text-muted-foreground">
            Stop Loss (points)
          </Label>
          <MultiSelect
            options={[
              { value: "null", label: "No stop loss" },
              ...options.stopLossValues.map((v) => ({
                value: String(v),
                label: String(v),
              })),
            ]}
            selected={(filters.stopLoss || []).map((v) =>
              v === null ? "null" : String(v)
            )}
            onChange={(vals) =>
              update({
                stopLoss: vals.map((v) => (v === "null" ? null : Number(v))),
              })
            }
            placeholder="All stop losses"
          />
        </div>
      )}

      {/* Asset */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Asset
        </Label>
        <MultiSelect
          options={options.assets.map((a) => ({
            value: a.toLowerCase(),
            label: a,
          }))}
          selected={filters.asset || []}
          onChange={(vals) => update({ asset: vals })}
          placeholder="All assets"
        />
      </div>

      <Separator />

      {/* Time of Day (hour range slider) */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Time of Day (UTC hour)
        </Label>
        <div className="px-1">
          <Slider
            min={0}
            max={23}
            step={1}
            value={hourRange}
            onValueChange={(val) =>
              update({ hourRange: [val[0], val[1]] as [number, number] })
            }
          />
        </div>
        <div className="flex justify-between text-xs text-muted-foreground">
          <span>{hourRange[0]}:00</span>
          <span>{hourRange[1]}:00</span>
        </div>
      </div>

      {/* Days of Week */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Days of Week
        </Label>
        <div className="grid grid-cols-2 gap-1">
          {options.daysOfWeek.map((d) => (
            <label
              key={d.value}
              className="flex items-center gap-1.5 text-xs cursor-pointer"
            >
              <Checkbox
                checked={(filters.daysOfWeek || []).includes(d.value)}
                onCheckedChange={(checked) => {
                  const current = filters.daysOfWeek || [];
                  if (checked) {
                    update({ daysOfWeek: [...current, d.value] });
                  } else {
                    update({
                      daysOfWeek: current.filter((v) => v !== d.value),
                    });
                  }
                }}
              />
              {d.label.slice(0, 3)}
            </label>
          ))}
        </div>
      </div>

      <Separator />

      {/* Time Remaining at Entry */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Time Remaining at Entry
        </Label>
        <MultiSelect
          options={options.timeRemainingBuckets.map((b) => ({
            value: b,
            label: b,
          }))}
          selected={filters.timeRemainingBucket || []}
          onChange={(vals) => update({ timeRemainingBucket: vals })}
          placeholder="All buckets"
        />
      </div>

      {/* Combined Entry Spread */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Combined Entry Spread
        </Label>
        <MultiSelect
          options={options.combinedSpreadBuckets.map((b) => ({
            value: b,
            label: b,
          }))}
          selected={filters.combinedSpreadBucket || []}
          onChange={(vals) => update({ combinedSpreadBucket: vals })}
          placeholder="All spreads"
        />
      </div>

      {/* Price Regime */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Price Regime
        </Label>
        <MultiSelect
          options={options.priceRegimes.map((r) => ({
            value: r,
            label: r,
          }))}
          selected={filters.priceRegime || []}
          onChange={(vals) => update({ priceRegime: vals })}
          placeholder="All regimes"
        />
      </div>

      {/* First Leg Cost (P1) — min/max in cents */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          First Leg Cost (¢)
        </Label>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <Label className="text-[10px] text-muted-foreground">
              Min
            </Label>
            <input
              type="number"
              min={0}
              max={99}
              placeholder="0"
              value={filters.minFirstLegCost ?? ""}
              onChange={(e) => {
                const v = e.target.value;
                update({
                  minFirstLegCost:
                    v === "" ? undefined : Math.min(99, Math.max(0, Number(v))),
                });
              }}
              className="w-full rounded-md border border-input bg-background px-2 py-1.5 text-sm"
            />
          </div>
          <div>
            <Label className="text-[10px] text-muted-foreground">
              Max
            </Label>
            <input
              type="number"
              min={0}
              max={99}
              placeholder="99"
              value={filters.maxFirstLegCost ?? ""}
              onChange={(e) => {
                const v = e.target.value;
                update({
                  maxFirstLegCost:
                    v === "" ? undefined : Math.min(99, Math.max(0, Number(v))),
                });
              }}
              className="w-full rounded-md border border-input bg-background px-2 py-1.5 text-sm"
            />
          </div>
        </div>
        <p className="text-[10px] text-muted-foreground">
          e.g. max 30 = buying at 30¢ or less
        </p>
      </div>

      <Separator />

      {/* Date Range */}
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Date After
        </Label>
        <input
          type="date"
          value={filters.dateAfter || ""}
          onChange={(e) => update({ dateAfter: e.target.value || undefined })}
          className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm"
        />
      </div>
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground">
          Date Before
        </Label>
        <input
          type="date"
          value={filters.dateBefore || ""}
          onChange={(e) => update({ dateBefore: e.target.value || undefined })}
          className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm"
        />
      </div>

      <Separator />

      <Button onClick={onApply} disabled={loading} className="w-full">
        {loading ? "Loading..." : "Apply Filters"}
      </Button>
    </div>
  );
}
