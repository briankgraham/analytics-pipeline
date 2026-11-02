// Creates every topic in config/local.yml that doesn't exist yet, and sets how long each topic keeps
// its messages. Run it with `pnpm topics` after `docker compose up -d --wait`. Safe to run again: a
// topic that already exists keeps its messages and gets the retention from the file, but a partition
// count that differs from the file is an error.
//
// It talks to Kafka through Kafka's own command-line tools, run inside the Kafka
// container with `docker compose exec`. That only reaches the local Kafka, so this script only
// reads the local config.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { partitionCountFrom, readTopics } from './topics.ts';

// Runs one of Kafka's command-line tools with these arguments and returns what it printed.
function runKafkaTool(tool: string, args: string[]): string {
  try {
    return execFileSync(
      'docker',
      [
        'compose',
        'exec',
        '-T',
        'kafka',
        `/opt/kafka/bin/${tool}`,
        '--bootstrap-server',
        '127.0.0.1:9092',
        ...args,
      ],
      {
        // Run from the project folder, where docker-compose.yml is, wherever the script is
        // started from.
        cwd: fileURLToPath(new URL('..', import.meta.url)),
        encoding: 'utf8',
        // Show Kafka's own error messages, but keep its normal output for us to read.
        stdio: ['ignore', 'pipe', 'inherit'],
      },
    );
  } catch {
    throw new Error(
      `${tool} ${args.join(' ')} failed (see above). ` +
        'Is Kafka running? Start it with: docker compose up -d --wait',
    );
  }
}

function kafkaTopics(args: string[]): string {
  return runKafkaTool('kafka-topics.sh', args);
}

const dayMs = 24 * 60 * 60 * 1000;

function createTopics(): void {
  const topics = readTopics(readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'));

  for (const topic of topics) {
    kafkaTopics([
      '--create',
      '--if-not-exists',
      '--topic',
      topic.name,
      '--partitions',
      String(topic.partitions),
      '--replication-factor',
      '1',
    ]);

    const actual = partitionCountFrom(
      kafkaTopics(['--describe', '--topic', topic.name]),
      topic.name,
    );
    if (actual !== topic.partitions) {
      throw new Error(
        `${topic.name} has ${actual} partitions but config/local.yml says ${topic.partitions}. ` +
          'To change it, wipe the data with: docker compose down -v',
      );
    }

    runKafkaTool('kafka-configs.sh', [
      '--alter',
      '--entity-type',
      'topics',
      '--entity-name',
      topic.name,
      '--add-config',
      `retention.ms=${topic.retentionDays * dayMs}`,
    ]);
    console.log(
      `ok  ${topic.name} (${topic.partitions} partitions, kept ${topic.retentionDays} days): ${topic.purpose}`,
    );
  }
}

// Print a problem as one plain line instead of a stack trace.
try {
  createTopics();
} catch (error) {
  console.error(`create-topics: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
