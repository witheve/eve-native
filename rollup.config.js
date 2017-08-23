import resolve from "rollup-plugin-node-resolve";
import commonjs from "rollup-plugin-commonjs";
import typescript from "rollup-plugin-typescript2";
import json from "rollup-plugin-json";
import multiEntry from "rollup-plugin-multi-entry";
import postcss from "rollup-plugin-postcss";

// Post CSS plugins
import autoprefixer from "autoprefixer";
import customProperties from "postcss-custom-properties";
import colorFunction from "postcss-color-function";

export default {
  entry: ["./ts/main.ts", "./libraries/**/*.css"],
  format: "iife",
  plugins: [
    multiEntry(),
    resolve({main: true, browser: true}),
    commonjs({
      namedExports: {
        "uuid": ["v4"]
      }
    }),
    typescript({}), // rollupCommonJSResolveHack: true
    json(),
    postcss({
      extract: "./dist/libraries.css",
      plugins: [
        autoprefixer(),
        customProperties(),
        colorFunction()
      ]
    }),
  ],
  moduleName: "eveNative",
  dest: "./dist/eve-native-bundle.js"
};
