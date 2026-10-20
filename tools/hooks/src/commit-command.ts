export type CommitCommand =
  { kind: 'none' } | { kind: 'commit'; messageFile: string } | { kind: 'deny'; reason: string };

export const ALLOWED_FORM =
  'stage everything first, then run exactly `git commit -F <file>` (optionally --amend, -q) as its own command';

// No --no-edit: the message git records must always be the file the hook checked.
const FLAGS = new Set(['--amend', '-q', '--quiet']);
const MESSAGE_FILE_FLAGS = new Set(['-F', '--file']);
// A plain path: no quoting, expansion, redirection, or separators; never `-` (stdin) or a flag.
const PLAIN_PATH = /^(?!-)[\w./@+-]+$/;

/**
 * Classifies a shell command for the Claude Code commit gate as an allowlist, not a denylist:
 * shell quoting, aliases, `-c` config, `-C`, `-a`, abbreviated `--no-verify`, env prefixes, and
 * chained commands make bypass detection unwinnable. Any command that mentions both git and commit
 * (after removing quotes and backslashes) must be exactly the allowed form, so the gate verifies
 * the tree that is committed, in the directory it is committed in, with the hooks intact.
 */
export function analyzeCommand(command: string): CommitCommand {
  const unquoted = command.replace(/["'\\]/g, '');
  if (!/\bgit\b/.test(unquoted) || !/\bcommit\b/.test(unquoted)) return { kind: 'none' };

  const deny = (why: string): CommitCommand => ({
    kind: 'deny',
    reason: `${why}; ${ALLOWED_FORM}`,
  });
  const tokens = command.trim().split(/\s+/);
  if (tokens[0] !== 'git' || tokens[1] !== 'commit') {
    return deny('a command mentioning git and commit must be a standalone `git commit`');
  }

  let messageFile: string | null = null;
  // One iterator, so `-F` can consume its argument and the loop continues after it.
  const args = tokens.slice(2)[Symbol.iterator]();
  for (const token of args) {
    const inline = /^--file=(.*)$/.exec(token);
    if (inline !== null || MESSAGE_FILE_FLAGS.has(token)) {
      const file = inline !== null ? inline[1] : args.next().value;
      if (file === undefined || !PLAIN_PATH.test(file)) {
        return deny(`${token} needs a plain file path, got ${JSON.stringify(file ?? '')}`);
      }
      messageFile = file;
    } else if (!FLAGS.has(token)) {
      return deny(`argument ${JSON.stringify(token)} is not allowed`);
    }
  }
  if (messageFile === null) {
    return deny('a message file is required (no editor, no -m)');
  }
  return { kind: 'commit', messageFile };
}
