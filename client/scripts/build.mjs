import {mkdir} from "node:fs/promises";
import {dirname, resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {build} from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = resolve(root, process.env.CLAVIRON_CHAT_PUBLIC_DIR || "./public");

const minify = process.env.CLAVIRON_CHAT_MINIFY === "1";
const outFile = resolve(outDir, minify ? "claviron-chat.min.js" : "claviron-chat.js");

await mkdir(outDir, {recursive: true});

await build({
    entryPoints: [resolve(root, "src/index.js")],
    bundle: true,
    format: "iife",
    globalName: "ClavironChat",
    target: "es2020",
    legalComments: "none",
    minify,
    outfile: outFile,
});

console.log(`built ${outFile}`);
