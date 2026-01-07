export type ExprNode =
  | { type: "literal"; value: number }
  | { type: "ref"; name: string }
  | { type: "binary"; op: "+" | "-" | "*" | "/"; left: ExprNode; right: ExprNode };

export function parseExpression(input: string): ExprNode {
  const tokens = tokenize(input);
  let pos = 0;

  function peek(): string | null {
    return pos < tokens.length ? tokens[pos] : null;
  }

  function consume(): string {
    return tokens[pos++]!;
  }

  function parsePrimary(): ExprNode {
    const t = peek();
    if (t === null) throw new Error("unexpected end of expression");
    if (t === "(") {
      consume();
      const node = parseExpr();
      if (peek() !== ")") throw new Error("missing closing parenthesis");
      consume();
      return node;
    }
    if (/^[A-Za-z][A-Za-z0-9_]*$/.test(t)) {
      consume();
      return { type: "ref", name: t };
    }
    if (/^[+-]?\\d+(\\.\\d+)?$/.test(t)) {
      consume();
      return { type: "literal", value: Number(t) };
    }
    throw new Error(`unexpected token '${t}'`);
  }

  function parseTerm(): ExprNode {
    let node = parsePrimary();
    while (peek() === "*" || peek() === "/") {
      const op = consume() as "*" | "/";
      const rhs = parsePrimary();
      node = { type: "binary", op, left: node, right: rhs };
    }
    return node;
  }

  function parseExpr(): ExprNode {
    let node = parseTerm();
    while (peek() === "+" || peek() === "-") {
      const op = consume() as "+" | "-";
      const rhs = parseTerm();
      node = { type: "binary", op, left: node, right: rhs };
    }
    return node;
  }

  const ast = parseExpr();
  if (pos !== tokens.length) {
    throw new Error(`unexpected token '${tokens[pos]}'`);
  }
  return ast;
}

export function exprToSql(ast: ExprNode): string {
  switch (ast.type) {
    case "literal":
      return String(ast.value);
    case "ref":
      return `value_for_alias('${ast.name}')`;
    case "binary": {
      const left = exprToSql(ast.left);
      const right = exprToSql(ast.right);
      return `(${left} ${ast.op} ${right})`;
    }
    default:
      throw new Error("unknown expression node");
  }
}

export function exprToSqlPivot(ast: ExprNode): string {
  switch (ast.type) {
    case "literal":
      return String(ast.value);
    case "ref":
      return ast.name;
    case "binary":
      return `(${exprToSqlPivot(ast.left)} ${ast.op} ${exprToSqlPivot(ast.right)})`;
    default:
      throw new Error("unknown expression node");
  }
}

export function collectRefs(ast: ExprNode): string[] {
  const refs: Set<string> = new Set();
  (function walk(node: ExprNode) {
    if (node.type === "ref") {
      refs.add(node.name);
    } else if (node.type === "binary") {
      walk(node.left);
      walk(node.right);
    }
  })(ast);
  return Array.from(refs);
}

function tokenize(input: string): string[] {
  const tokens: string[] = [];
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (ch === " " || ch === "\\t" || ch === "\\n") {
      i++;
      continue;
    }
    if ("()+-*/".includes(ch)) {
      tokens.push(ch);
      i++;
      continue;
    }
    const identMatch = /^[A-Za-z][A-Za-z0-9_]*/.exec(input.slice(i));
    if (identMatch) {
      tokens.push(identMatch[0]);
      i += identMatch[0].length;
      continue;
    }
    const numMatch = /^[+-]?\\d+(\\.\\d+)?/.exec(input.slice(i));
    if (numMatch) {
      tokens.push(numMatch[0]);
      i += numMatch[0].length;
      continue;
    }
    throw new Error(`invalid character '${ch}'`);
  }
  return tokens;
}
