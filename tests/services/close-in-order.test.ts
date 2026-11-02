import { describe, expect, it } from 'vitest';
import { closeInOrder } from '../../services/close-in-order.ts';

describe('closeInOrder', () => {
  it('runs every step in order', async () => {
    const order: number[] = [];
    await closeInOrder([async () => void order.push(1), async () => void order.push(2)]);
    expect(order).toEqual([1, 2]);
  });

  it('keeps going after a failure, then throws that failure', async () => {
    const order: number[] = [];
    const closing = closeInOrder([
      async () => {
        throw new Error('stuck');
      },
      async () => void order.push(2),
    ]);
    await expect(closing).rejects.toThrow('stuck');
    expect(order).toEqual([2]);
  });

  it('reports every failure when there are several', async () => {
    const fail = (message: string) => async () => {
      throw new Error(message);
    };
    const closing = closeInOrder([fail('first'), fail('second')]);
    await expect(closing).rejects.toBeInstanceOf(AggregateError);
    await expect(closing).rejects.toMatchObject({
      errors: [
        expect.objectContaining({ message: 'first' }),
        expect.objectContaining({ message: 'second' }),
      ],
    });
  });
});
