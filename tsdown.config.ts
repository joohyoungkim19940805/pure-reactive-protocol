import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    core: "src/core.ts",
    transport: "src/transport.ts",
    browser: "src/browser.ts",
    node: "src/node.ts",
    "profile-rpc": "src/profile-rpc.ts",
    rxjs: "src/rxjs.ts",
    "compatibility-rsocket-v1": "src/compatibility-rsocket-v1.ts",
    "compatibility-rsocket-v1-node": "src/compatibility-rsocket-v1-node.ts"
  },
  format: ["esm", "cjs"],
  platform: "neutral",
  target: "es2022",
  dts: true,
  clean: true,
  sourcemap: true,
  outExtensions: ({ format }) => ({
    js: format === "cjs" ? ".cjs" : ".js"
  }),
  deps: {
    neverBundle: ["rxjs"]
  }
});
