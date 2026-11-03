import { describe, expect, it, vi } from 'vitest';
import type { LogCriterion } from '../../../elasticsearch/build-logs-query.ts';
import { createLogSearcher } from '../../../api/services/search-service.ts';

const log = { service: 'checkout', level: 'error', message: 'card declined' };
const criteria: LogCriterion[] = [{ type: 'exact', field: 'level', value: 'error' }];

describe('searchLogs', () => {
  it('searches the given index, limits the results, and answers with the logs', async () => {
    const searchDocuments = vi.fn(async () => [log]);
    const searchLogs = createLogSearcher({ searchDocuments }, 'logs');

    await expect(searchLogs(criteria)).resolves.toEqual([log]);
    expect(searchDocuments).toHaveBeenCalledWith('logs', {
      query: { bool: { must: [], filter: [{ term: { level: 'error' } }] } },
      size: 50,
    });
  });

  it('lets a failure through, so the caller can answer that it failed', async () => {
    const searchDocuments = vi.fn(async () => {
      throw new Error('too busy');
    });
    const searchLogs = createLogSearcher({ searchDocuments }, 'logs');
    await expect(searchLogs(criteria)).rejects.toThrow('too busy');
  });
});
