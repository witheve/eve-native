import resolve from "rollup-plugin-node-resolve";
import commonjs from "rollup-plugin-commonjs";
import typescript from "rollup-plugin-typescript2";
import json from "rollup-plugin-json";

export default {
  entry: "./ts/main.ts",
  format: "iife",
  plugins: [
    resolve({main: true, browser: true}),
    commonjs({
      namedExports: {
        "uuid": ["v4"]
      }
    }),
    typescript({}), // rollupCommonJSResolveHack: true
    json(),
  ],
  moduleName: "eveNative",
  dest: "./dist/eve-native-bundle.js"
}
