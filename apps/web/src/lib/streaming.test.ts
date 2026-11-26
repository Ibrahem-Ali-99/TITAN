import test from "node:test";
import assert from "node:assert/strict";

import { distributionBarWidth } from "./streaming.ts";

test("distribution bars render proportional nonzero fills and empty zero values", () => {
  assert.equal(distributionBarWidth(50, 100), "50%");
  assert.equal(distributionBarWidth(1, 100), "8%");
  assert.equal(distributionBarWidth(0, 100), "0%");
  assert.equal(distributionBarWidth(10, 0), "0%");
});
