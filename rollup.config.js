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

function createBundle(name, entry, moduleName) {
  name = moduleName
  return {
    moduleName,
    entry,
    dest: `./dist/${name}.js`,

    format: "iife",
    plugins: [
      multiEntry(),
      resolve({main: true, browser: true}),
      commonjs({
        namedExports: {
          "uuid": ["v4"]
        }
      }),
      typescript({}),
      json(),
      postcss({
        extract: `./dist/${name}.css`,
        plugins: [
          autoprefixer(),
          customProperties(),
          colorFunction()
        ]
      }),
    ],
  }
}

export default [
  createBundle("eve-libraries", ["./ts/main.ts", "./libraries/**/*.css"], "eve_libraries"),
  createBundle("programs/editor", ["./examples/editor/**/*.css"], "eve_programs_editor")
];
