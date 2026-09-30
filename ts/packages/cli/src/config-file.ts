import { stat } from "node:fs/promises";
import { dirname, isAbsolute, join, relative } from "node:path";

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

/**
 * Figment's file source label. A path relative to `cwd` is used when it has
 * fewer components than the absolute path.
 */
export function figmentFileSource(located: string, cwd = process.cwd()): string {
  const rel = relative(cwd, located);
  if (rel.length > 0 && pathComponents(rel) < pathComponents(located)) {
    return rel;
  }
  return located;
}

/** A Figment extract failure names the file. A TOML parse error does not. */
export function wantsFigmentLocation(detail: string): boolean {
  return (
    detail.startsWith("invalid type:") ||
    detail.startsWith("invalid value ") ||
    detail.startsWith("number too large to fit in target type")
  );
}

function pathComponents(path: string): number {
  const parts = path.split("/").filter((part) => part.length > 0);
  return (path.startsWith("/") ? 1 : 0) + parts.length;
}
