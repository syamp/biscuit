#!/usr/bin/env node
const esbuild = require('esbuild');
const path = require('path');

async function main() {
  const commonLoaders = { '.ts': 'ts', '.tsx': 'tsx', '.css': 'css' };

  await esbuild.build({
    entryPoints: [path.join('frontend', 'dashboard.tsx')],
    outfile: path.join('static', 'dashboard.js'),
    bundle: true,
    minify: true,
    sourcemap: false,
    target: 'es2017',
    format: 'iife',
    globalName: 'TSDBApp',
    loader: commonLoaders
  });
  console.log('Built static/dashboard.js');

  await esbuild.build({
    entryPoints: [path.join('frontend', 'style.css')],
    outfile: path.join('static', 'dashboard.css'),
    bundle: true,
    minify: true,
    sourcemap: false,
    loader: commonLoaders
  });
  console.log('Built static/dashboard.css');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
