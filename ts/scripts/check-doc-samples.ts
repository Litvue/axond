/**
 * Run every TypeScript sample in docs/typescript.md and syntax-check every
 * bash sample. The samples are the mount guide a new caller follows.
 */
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

const repo = new URL("../../", import.meta.url).pathname;
const doc = await readFile(new URL("../../docs/typescript.md", import.meta.url), "utf8");
const fences = [...doc.matchAll(/```(ts|bash)\n([\s\S]*?)```/g)];
if (!fences.some((fence) => fence[1] === "ts")) {
  process.stderr.write("docs/typescript.md has no TypeScript sample\n");
  process.exit(1);
}

const directory = await mkdtemp(join(repo, "ts/.samples-"));
let index = 0;
try {
  for (const fence of fences) {
    index += 1;
    const kind = fence[1];
    const body = fence[2] ?? "";
    if (kind === "ts") {
      const file = join(directory, `sample-${index}.ts`);
      await writeFile(file, body);
      await run(process.execPath, ["--experimental-strip-types", "--disable-warning=ExperimentalWarning", file]);
      continue;
    }
    const file = join(directory, `sample-${index}.sh`);
    await writeFile(file, body);
    await run("bash", ["-n", file]);
  }
} finally {
  await rm(directory, { recursive: true, force: true });
}
process.stdout.write(`doc samples: ${fences.length} fences ok\n`);

function run(command: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: join(repo, "ts"), stdio: ["ignore", "inherit", "inherit"] });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(" ")} exited ${code}`));
    });
  });
}
