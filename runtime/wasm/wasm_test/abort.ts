import "allocator/arena";

export { memory };

export function abort(): void {
  json.toI64("1")
  assert(false, "not true")
}

// Needed because we require an import section.
declare namespace json {
  function toI64(decimal: string): i64
}
