interface Atom {
  kind: "lit" | "dot" | "class";
  value: string;
  negated?: boolean;
  min: number;
  max: number;
}


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
    if ("()|^$".includes(char)) throw new Error("pattern rejected: grouping, alternation, and anchors are unsupported");
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
  if (!Number.isInteger(min) || !Number.isInteger(max) || min < 0 || max < min || max > 64) {
    throw new Error("pattern rejected: bad quantifier");
  }
  return { min, max, end: end + 1 };
}

function matchesEmpty(atoms: Atom[]): boolean {
  return atoms.every((atom) => atom.min === 0);
}

function matchAll(atoms: Atom[], input: string): Array<{ start: number; end: number }> {
  // Each (atom, input offset) is evaluated once. Quantifiers have a fixed 64
  // character bound, so accepted patterns cannot trigger exponential retries.
  let next = new Int32Array(input.length + 1);
  for (let i = 0; i <= input.length; i++) next[i] = i;
  for (let atomAt = atoms.length - 1; atomAt >= 0; atomAt--) {
    const atom = atoms[atomAt]!;
    const current = new Int32Array(input.length + 1).fill(-1);
    for (let offset = input.length; offset >= 0; offset--) {
      let maximum = 0;
      while (maximum < atom.max && offset + maximum < input.length && accepts(atom, input[offset + maximum]!)) maximum++;
      for (let count = maximum; count >= atom.min; count--) {
        if (next[offset + count]! >= 0) { current[offset] = next[offset + count]!; break; }
      }
    }
    next = current;
  }
  const found: Array<{ start: number; end: number }> = [];
  for (let offset = 0; offset < input.length;) {
    const end = next[offset]!;
    if (end > offset) { found.push({ start: offset, end }); offset = end; }
    else offset++;
  }
  return found;
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
