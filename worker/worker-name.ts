// What this worker calls itself in everything it prints, and the name Kafka knows it by. Workers can run
// side by side, so the process id tells them apart.
export const workerName = `worker-${process.pid}`;
