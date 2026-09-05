import { copyFile, mkdir } from "node:fs/promises";

const publicDir = new URL("../public/", import.meta.url);
await mkdir(publicDir, { recursive: true });
for (const name of ["install.sh", "install.ps1"]) {
  await copyFile(
    new URL("../../" + name, import.meta.url),
    new URL(name, publicDir),
  );
}
