// Creates every topic in config/local.yml that doesn't exist yet. Run it with `pnpm topics` after
// `docker compose up -d --wait`. Safe to run again: topics that already exist are left alone, but
// a partition count that differs from the file is an error.
//
// It talks to Kafka through Kafka's own command-line tool, kafka-topics.sh, run inside the Kafka
// container with `docker compose exec`. That only reaches the local Kafka, so this script only
// reads the local config.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { partitionCountFrom, readTopics } from './topics.ts';

// Runs kafka-topics.sh with these arguments and returns what it printed.
function kafkaTopics(args: string[]): string {
  try {
    return execFileSync(
      'docker',
      [
        'compose',
        'exec',
        '-T',
        'kafka',
        '/opt/kafka/bin/kafka-topics.sh',
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
      `kafka-topics.sh ${args.join(' ')} failed (see above). ` +
        'Is Kafka running? Start it with: docker compose up -d --wait',
    );
  }
}

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
    console.log(`ok  ${topic.name} (${topic.partitions} partitions): ${topic.purpose}`);
  }
}

// Print a problem as one plain line instead of a stack trace.
try {
  createTopics();
} catch (error) {
  console.error(`create-topics: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
