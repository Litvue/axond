import { stat } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

/**
 * Figment's `Toml::file` lookup. An absolute path is that file or nothing.
 * A relative path is sought from `cwd` upward until a regular file is found.
 * A missing path is an empty document, not an I/O error.
 */
export async function locateConfigFile(operatorPath: string, cwd = process.cwd()): Promise<string | null> {
  if (isAbsolute(operatorPath)) {
    return (await isFile(operatorPath)) ? operatorPath : null;
  }
  let dir = cwd;
  for (;;) {
    const candidate = join(dir, operatorPath);
    if (await isFile(candidate)) {
      return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) {
      return null;
    }
    dir = parent;
  }
}
