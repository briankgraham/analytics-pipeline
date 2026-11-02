// Runs each closing step in order. A step that fails doesn't stop the ones after it, because something
// that failed to close is no reason to leave everything else open. Throws at the end if any step failed.
export async function closeInOrder(steps: (() => Promise<void>)[]): Promise<void> {
  const problems: unknown[] = [];
  for (const step of steps) {
    try {
      await step();
    } catch (error) {
      problems.push(error);
    }
  }
  if (problems.length === 1) {
    throw problems[0];
  }
  if (problems.length > 1) {
    throw new AggregateError(problems, 'several things could not close');
  }
}
