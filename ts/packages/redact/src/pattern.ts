interface Atom {
  kind: "lit" | "dot" | "class";
  value: string;
  negated?: boolean;
  min: number;
  max: number;
}

const BUDGET_FACTOR = 8;

/** Compile an operator pattern. Nested quantifiers and empty matches are refused. */
export function compilePattern(source: string): (input: string) => Array<{ start: number; end: number }> {
  if (hasNestedQuantifier(source)) {
    throw new Error(`pattern rejected: nested quantifiers are not allowed (${source})`);
  }
  const atoms = parse(source);
  if (matchesEmpty(atoms)) {
    throw new Error("pattern rejected: empty matches are not allowed");
  }
  const probe = "a".repeat(32);
  const found = matchAll(atoms, probe);
  void found;
  return (input: string) => matchAll(atoms, input);
}

function hasNestedQuantifier(source: string): boolean {
  let depth = 0;
  let quantifierAtDepth: number[] = [];
  for (let index = 0; index < source.length; index += 1) {
    const char = source[index]!;
    if (char === "\\") {
      index += 1;
      continue;
    }
    if (char === "(") {
      depth += 1;
      quantifierAtDepth[depth] = 0;
    } else if (char === ")") {
      const inner = quantifierAtDepth[depth] ?? 0;
      depth -= 1;
      const next = source[index + 1];
      if (inner > 0 && next && "*+?{".includes(next)) {
        return true;
      }
    } else if ("*+?".includes(char) || char === "{") {
      quantifierAtDepth[depth] = (quantifierAtDepth[depth] ?? 0) + 1;
    }
  }
  return false;
}

function parse(source: string): Atom[] {
  const atoms: Atom[] = [];
  let index = 0;
  while (index < source.length) {
    const char = source[index]!;
    if (char === "(" || char === ")") {
      index += 1;
      continue;
    }
    let atom: Atom;
    if (char === "\\") {
      atom = { kind: "lit", value: source[index + 1] ?? "", min: 1, max: 1 };
      index += 2;
    } else if (char === ".") {
      atom = { kind: "dot", value: "", min: 1, max: 1 };
      index += 1;
    } else if (char === "[") {
      const end = source.indexOf("]", index + 1);
      if (end === -1) {
        throw new Error("pattern rejected: unclosed class");
      }
      const raw = source.slice(index + 1, end);
      atom = { kind: "class", value: raw.startsWith("^") ? raw.slice(1) : raw, negated: raw.startsWith("^"), min: 1, max: 1 };
      index = end + 1;
    } else if ("*+?{".includes(char)) {
      throw new Error("pattern rejected: quantifier without an atom");
    } else {
      atom = { kind: "lit", value: char, min: 1, max: 1 };
      index += 1;
    }
    const quant = readQuantifier(source, index);
    if (quant) {
      atom.min = quant.min;
      atom.max = quant.max;
      index = quant.end;
    }
    atoms.push(atom);
  }
  return atoms;
}

function readQuantifier(source: string, index: number): { min: number; max: number; end: number } | null {
  const char = source[index];
  if (char === "*") {
    return { min: 0, max: 64, end: index + 1 };
  }
  if (char === "+") {
    return { min: 1, max: 64, end: index + 1 };
  }
  if (char === "?") {
    return { min: 0, max: 1, end: index + 1 };
  }
  if (char !== "{") {
    return null;
  }
  const end = source.indexOf("}", index);
  if (end === -1) {
    throw new Error("pattern rejected: unclosed quantifier");
  }
  const body = source.slice(index + 1, end);
  const [minText, maxText] = body.split(",");
  const min = Number(minText);
  const max = maxText === undefined ? min : maxText.length === 0 ? 64 : Number(maxText);
  if (!Number.isInteger(min) || !Number.isInteger(max) || min < 0 || max < min) {
    throw new Error("pattern rejected: bad quantifier");
  }
  return { min, max, end: end + 1 };
}

function matchesEmpty(atoms: Atom[]): boolean {
  return atoms.every((atom) => atom.min === 0);
}

function matchAll(atoms: Atom[], input: string): Array<{ start: number; end: number }> {
  const found: Array<{ start: number; end: number }> = [];
  let index = 0;
  const budget = input.length * Math.max(atoms.length, 1) * BUDGET_FACTOR + 32;
  let steps = 0;
  const tick = () => {
    steps += 1;
    if (steps > budget) {
      throw new Error("pattern exceeded its linear step budget");
    }
  };
  while (index <= input.length) {
    const hit = matchAt(atoms, input, index, 0, tick);
    if (hit !== null && hit > index) {
      found.push({ start: index, end: hit });
      index = hit;
      continue;
    }
    index += 1;
  }
  return found;
}

function matchAt(atoms: Atom[], input: string, inputAt: number, atomAt: number, tick: () => void): number | null {
  tick();
  if (atomAt === atoms.length) {
    return inputAt;
  }
  const atom = atoms[atomAt]!;
  for (let count = atom.max; count >= atom.min; count -= 1) {
    let cursor = inputAt;
    let ok = true;
    for (let taken = 0; taken < count; taken += 1) {
      if (cursor >= input.length || !accepts(atom, input[cursor]!)) {
        ok = false;
        break;
      }
      cursor += 1;
    }
    if (!ok) {
      continue;
    }
    const rest = matchAt(atoms, input, cursor, atomAt + 1, tick);
    if (rest !== null) {
      return rest;
    }
  }
  return null;
}

function accepts(atom: Atom, char: string): boolean {
  if (atom.kind === "dot") {
    return char !== "\n";
  }
  if (atom.kind === "lit") {
    return char === atom.value;
  }
  const hit = classHas(atom.value, char);
  return atom.negated ? !hit : hit;
}

function classHas(body: string, char: string): boolean {
  for (let index = 0; index < body.length; index += 1) {
    if (body[index + 1] === "-" && index + 2 < body.length) {
      if (char >= body[index]! && char <= body[index + 2]!) {
        return true;
      }
      index += 2;
      continue;
    }
    if (body[index] === char) {
      return true;
    }
  }
  return false;
}
