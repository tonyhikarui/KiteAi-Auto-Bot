import fetch from 'node-fetch';
import chalk from 'chalk';
import fs from 'fs/promises';
import { Worker } from 'worker_threads';
import mysql from 'mysql2/promise';
import { banner } from './banner.js';

// Add loadProxies function back before CONFIG definition
async function loadProxies() {
    try {
        const data = await fs.readFile('proxies.txt', 'utf8');
        return data.split('\n')
            .map(line => line.trim())
            .filter(line => line && !line.startsWith('#'))
            .map(proxy => {
                if (proxy.includes('://')) {
                    const url = new URL(proxy);
                    const protocol = url.protocol.replace(':', '');
                    const auth = url.username ? `${url.username}:${url.password}` : '';
                    const host = url.hostname;
                    const port = url.port;
                    return { protocol, host, port, auth };
                } else {
                    const parts = proxy.split(':');
                    let [protocol, host, port, user, pass] = parts;
                    protocol = protocol.replace('//', '');
                    const auth = user && pass ? `${user}:${pass}` : '';
                    return { protocol, host, port, auth };
                }
            });
    } catch (err) {
        console.log(chalk.yellow('[INFO] No proxies.txt found or error reading file. Using direct connection.'));
        return [];
    }
}

const CONFIG = {
    BATCH_SIZE: 30,
    NUM_WORKERS: 30,
    START_OFFSET: 0,
    PROCESS_AMOUNT: 2000,
    WORKER_DELAY: 1,
    WALLET_DELAY: 1,
    WORKER_TIMEOUT: 1500000  // Increase timeout to 5 minutes
};

const DB_CONFIG = {
    host: 'localhost',
    user: 'ledge',
    password: 'hfLEsAtStG4LzETZ',
    database: 'ledge'
};

// Add after DB_CONFIG and before workers declaration
async function loadWalletsFromDB(offset = 0, limit = 100) {
  const connection = await mysql.createConnection(DB_CONFIG);
  try {
    const [rows] = await connection.execute(
      'SELECT address, privateKey FROM wallets LIMIT ? OFFSET ?',
      [limit, offset]
    );
    return rows;
  } finally {
    await connection.end();
  }
}

// Global worker management
const workers = new Map();
const taskQueue = [];

async function delay(seconds) {
    return new Promise(resolve => setTimeout(resolve, seconds * 1000));
}

async function getTotalWallets() {
    const connection = await mysql.createConnection(DB_CONFIG);
    try {
        const [rows] = await connection.execute('SELECT COUNT(*) as total FROM wallets');
        return rows[0].total;
    } finally {
        await connection.end();
    }
}

function enqueueTask(task) {
    taskQueue.push(task);
}

// Add cleanup helper
async function cleanupWorker(worker, workerIndex) {
    try {
        worker.removeAllListeners();
        await worker.terminate();
        workers.delete(workerIndex);
        console.log(chalk.yellow(`Worker ${workerIndex} cleaned up`));
    } catch (error) {
        console.error(chalk.red(`Error cleaning up worker ${workerIndex}:`, error));
    }
}

// Fix worker message handling in processTaskQueue
async function processTaskQueue(proxies, errorCount, totalProcessed) {
    const taskPromises = taskQueue.splice(0, taskQueue.length).map(async task => {
        const { wallet, workerIndex, offset, proxiesIndex, currentCount } = task;
        const proxy = proxies[proxiesIndex % proxies.length] || null;
        const worker = workers.get(workerIndex);

        if (!worker) {
            console.log(chalk.red(`Worker ${workerIndex} not found`));
            errorCount.value++;
            return;
        }

        try {
            await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    worker.removeAllListeners('message');
                    reject(new Error('Worker timeout'));
                }, CONFIG.WORKER_TIMEOUT);

                const messageHandler = async (message) => {
                    clearTimeout(timeout);
                    worker.removeListener('message', messageHandler);

                    if (message.type === 'complete') {
                        console.log(chalk.green(`Wallet ${wallet.address} processed successfully in ${message.processTime}s`));
                        resolve();
                    } else if (message.type === 'error') {
                        console.log(chalk.red(`Error processing wallet ${wallet.address} (${message.processTime}s): ${message.data}`));
                        reject(new Error(message.data));
                    }
                };

                worker.on('message', messageHandler);
                worker.postMessage({
                    type: 'process',
                    data: {
                        wallet: wallet.address, // Send only the address
                        proxy,
                        index: offset + 1,
                        total: CONFIG.PROCESS_AMOUNT,
                        currentCount
                    }
                });
            });
        } catch (error) {
            console.log(chalk.red(`Worker ${workerIndex}: Error processing wallet ${wallet.address}: ${error.message}`));
            errorCount.value++;
            await cleanupWorker(worker, workerIndex);
            await initializeWorker(workerIndex);
        }
        await delay(CONFIG.WALLET_DELAY);
    });

    await Promise.all(taskPromises);
}

// Split worker initialization into separate function
async function initializeWorker(index) {
    const worker = new Worker('./worker.js', {
        workerData: {
            workerIndex: index,
            totalWorkers: CONFIG.NUM_WORKERS
        }
    });

    worker.setMaxListeners(0); // Prevent memory leaks from too many listeners
    
    worker.on('error', (error) => {
        console.log(chalk.red(`Worker ${index} error: ${error}`));
    });

    worker.on('exit', (code) => {
        if (code !== 0) {
            console.log(chalk.red(`Worker ${index} stopped with code ${code}`));
        }
        workers.delete(index);
    });

    workers.set(index, worker);
    console.log(chalk.cyan(`Initialized worker ${index}`));
    await delay(CONFIG.WORKER_DELAY);
    
    return worker;
}

async function initializeWorkers() {
    for (let i = 0; i < CONFIG.NUM_WORKERS; i++) {
        await initializeWorker(i);
    }
}

async function main() {
    console.clear();
    console.log(banner);
    
    const proxyList = await loadProxies();
    console.log(chalk.cyan(`📊 Configuration:
    Batch Size: ${CONFIG.BATCH_SIZE}
    Workers: ${CONFIG.NUM_WORKERS}
    Start Offset: ${CONFIG.START_OFFSET}
    Worker Delay: ${CONFIG.WORKER_DELAY}s
    Wallet Delay: ${CONFIG.WALLET_DELAY}s
    Worker Timeout: ${CONFIG.WORKER_TIMEOUT / 1000}s
    `));

    let runCount = 1;
    await initializeWorkers();

    while (true) {
        const startTime = Date.now();
        let offset = CONFIG.START_OFFSET;
        let totalProcessed = 0;
        let errorCount = { value: 0 };

        //CONFIG.PROCESS_AMOUNT = await getTotalWallets();
        console.log(chalk.cyan(`Starting Run #${runCount} - Total wallets: ${CONFIG.PROCESS_AMOUNT}`));

        while (offset < CONFIG.PROCESS_AMOUNT) {
            const currentTotal = await getTotalWallets();
            if (currentTotal !== CONFIG.PROCESS_AMOUNT) {
                CONFIG.PROCESS_AMOUNT = currentTotal;
            }

            const remainingWallets = CONFIG.PROCESS_AMOUNT - offset;
            const currentBatchSize = Math.min(CONFIG.BATCH_SIZE, remainingWallets);
            const wallets = await loadWalletsFromDB(offset, currentBatchSize);

            if (!wallets || wallets.length === 0) {
                console.log(chalk.yellow('\n🏁 All wallets processed. Exiting...'));
                break;
            }

            // Enqueue tasks for each wallet
            for (let i = 0; i < wallets.length; i++) {
                const wallet = wallets[i];
                const workerIndex = i % CONFIG.NUM_WORKERS;
                const currentCount = totalProcessed + i + 1;
                enqueueTask({ wallet, workerIndex, offset, proxiesIndex: i, currentCount });
            }

            await processTaskQueue(proxyList, errorCount, totalProcessed);
            
            totalProcessed += wallets.length;
            offset += wallets.length;

            if (totalProcessed % 100 === 0) {
                const progress = ((offset / CONFIG.PROCESS_AMOUNT) * 100).toFixed(1);
                console.log(chalk.cyan(`Progress: ${progress}% (${offset}/${CONFIG.PROCESS_AMOUNT} wallets)`));
            }

            await delay(CONFIG.WORKER_DELAY);
        }

        const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
        console.log(chalk.green(`
Run #${runCount} completed:
- Total wallets: ${totalProcessed}
- Total time: ${totalTime} minutes
- Avg time per wallet: ${(totalTime / totalProcessed).toFixed(2)} minutes
- Errors: ${errorCount.value}
        `));

        runCount++;
        await delay(3600); // 1 hour delay between runs
    }
}

// Update termination handler with proper cleanup
process.on('SIGINT', async () => {
    console.log(chalk.yellow('\n🛑 Gracefully shutting down workers...'));
    for (const [index, worker] of workers.entries()) {
        await cleanupWorker(worker, index);
    }
    process.exit(0);
});

// Global error handler
process.on('unhandledRejection', (error) => {
    console.error(`\n${chalk.red('❌ Unhandled rejection:')} ${error.message}`);
});

main().catch(error => {
    console.error(`\n${chalk.red('❌ Fatal error:')} ${error.message}`);
    process.exit(1);
});
