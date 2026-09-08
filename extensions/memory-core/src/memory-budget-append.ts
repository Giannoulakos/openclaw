import { compactMemoryForBudget, type CompactMemoryParams } from "./memory-budget.js";

/** Compose the complete post-promotion file from the bounded compaction result. */
export function buildBudgetedMemoryAppend(params: CompactMemoryParams): {
  content: string;
  droppedDates: string[];
} {
  const compaction = compactMemoryForBudget(params);
  const header = compaction.compacted.trim().length > 0 ? "" : "# Long-Term Memory\n\n";
  const base =
    compaction.compacted.length === 0 || compaction.compacted.endsWith("\n")
      ? compaction.compacted
      : `${compaction.compacted}\n`;
  return {
    content: `${header}${base}${params.newSection}`,
    droppedDates: compaction.droppedDates,
  };
}

/** Return whether an append can fit without rewriting preserved content. */
export function canMemoryAppendEverFit(params: CompactMemoryParams): boolean {
  if (params.budgetChars <= 0) {
    return true;
  }
  return (
    buildBudgetedMemoryAppend({ ...params, maxPriorEntryLossFraction: 1 }).content.length <=
    params.budgetChars
  );
}
