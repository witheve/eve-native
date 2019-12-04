var webpack = require("webpack");
var path = require("path");
var MiniCssExtractPlugin = require("mini-css-extract-plugin");

const ASSET_PATH = process.env.ASSET_PATH || "/dist/";

// Post CSS plugins
var autoprefixer = require("autoprefixer");
var customProperties = require("postcss-custom-properties");
var colorFunction = require("postcss-color-function");

//----------------------------------------------------------------------
// Client Build
//----------------------------------------------------------------------

function make_default_config_for_entry(entry, params) {
  let {name = "[name]", no_splitting = false, no_hmr = false, no_compile_fluorine = false} = params;
  return {
    entry,
    output: {
      filename: `${name}.js`,
      path: path.resolve(__dirname, "dist"),
      publicPath: ASSET_PATH
    },
    devServer: {
      contentBase: ".",
      port: 8080
    },

    node: {
      fs: "empty"
    },

    devtool: "inline-source-map",
    resolve: {
      extensions: [".webpack.js", ".web.js", ".js", ".ts", ".tsx", ".json", ".css"]
    },

    plugins: [
      new webpack.NamedModulesPlugin(),
      new webpack.HotModuleReplacementPlugin(),
      new webpack.DefinePlugin({
        "process.env.ASSET_PATH": JSON.stringify(ASSET_PATH)
      }),
      new MiniCssExtractPlugin({
        filename: `./${name}.css`
      })
    ],

    module: {
      rules: [
        {test: /\.ts$/, use: "awesome-typescript-loader"},
        no_compile_fluorine
          ? {test: /\.tsx$/, loaders: ["awesome-typescript-loader"]}
          : {
              test: /\.tsx$/,
              loaders: ["fluorine-loader?benchmark=true", "awesome-typescript-loader"]
            },
        {test: /\.css$/, use: [MiniCssExtractPlugin.loader, "css-loader", {loader: "postcss-loader", options: {
          plugins: [
            autoprefixer(),
            customProperties(),
            colorFunction()
          ]
        }}]},
        {test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/, loader: "url-loader?limit=10000"},
        {enforce: "pre", test: /\.js$/, loader: "source-map-loader"}
      ]
    },

    optimization: no_splitting
      ? undefined
      : {
          namedChunks: true,
          splitChunks: {
            chunks: "all"
          }
        },

    devServer: no_hmr
      ? undefined
      : {
          hot: true,
          inline: true,
          headers: {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, OPTIONS",
            "Access-Control-Allow-Headers": "X-Requested-With, content-type, Authorization"
          }
        }
  };
}

module.exports = [
  make_default_config_for_entry("./libraries/eve-libraries.css", {name: "eve-libraries", no_compile_fluorine: true}),
  make_default_config_for_entry("./ts/main.ts", {name: "eve-libraries", no_compile_fluorine: true})
  // "./libraries/**/*.css"
];


//tsc 2.4.1
