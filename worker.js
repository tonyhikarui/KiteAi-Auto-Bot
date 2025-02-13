import { parentPort, workerData } from 'worker_threads';
import chalk from 'chalk';
import { KiteAIAutomation } from './kite-automation.js';

const CONFIG = {
    WALLET_DELAY: 2
};

class WorkerProgress {
    constructor(total) {
        this.total = total;
        this.current = 0;
        this.successful = 0;
        this.failed = 0;
        this.startTime = Date.now();
    }

    update(success) {
        this.current++;
        success ? this.successful++ : this.failed++;
        this.reportProgress();
    }

    reportProgress() {
        const elapsed = (Date.now() - this.startTime) / 1000;
        const progress = (this.current / this.total) * 100;
        
        parentPort.postMessage({
            type: 'progress',
            data: {
                total: this.total,
                current: this.current,
                successful: this.successful,
                failed: this.failed,
                progress: progress.toFixed(2),
                elapsed: elapsed.toFixed(1),
                remaining: this.estimateRemaining(elapsed)
            }
        });
    }

    estimateRemaining(elapsed) {
        if (this.current === 0) return 'calculating...';
        const rate = elapsed / this.current;
        const remaining = (this.total - this.current) * rate;
        return remaining.toFixed(1);
    }
}

async function processWallet(data) {
    const { wallet, proxy, index, total, currentCount } = data;
    const startTime = Date.now();
    
    console.log(chalk.cyan(`\n[Worker ${workerData.workerIndex}] Processing wallet ${currentCount}/${total}`));
    console.log(chalk.yellow(`Wallet Address: ${wallet}`));
    
    try {
        const instance = new KiteAIAutomation(wallet, proxy ? [proxy] : [], `${workerData.workerIndex}-${wallet.slice(0, 6)}`);
        await instance.run();
        
        const processTime = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(chalk.blue(`[Worker ${workerData.workerIndex}] Processing time: ${processTime}s`));
        
        return true;
    } catch (error) {
        const processTime = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(chalk.red(`[Worker ${workerData.workerIndex}] Failed after ${processTime}s`));
        throw new Error(`Failed to process wallet: ${error.message}`);
    }
}

// Add cleanup handler
function cleanup() {
    parentPort.removeAllListeners();
    process.removeAllListeners();
}

// Initialize progress tracking
const progress = new WorkerProgress(0); // Will be updated when tasks arrive

// Update message handling with cleanup
parentPort.on('message', async (task) => {
    if (task.type === 'stop') {
        cleanup();
        process.exit(0);
    }
    
    if (task.type === 'process') {
        const startTime = Date.now();
        try {
            const { wallet, proxy, walletIndex, totalWallets } = task.data;
            const automation = new KiteAIAutomation(
                wallet,
                proxy ? [proxy] : [],
                `${workerData.workerIndex}-${wallet.slice(0, 6)}`,
                walletIndex,
                totalWallets
            );
            await automation.run();
            const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
            parentPort.postMessage({ 
                type: 'complete',
                success: true,
                processTime: totalTime
            });
            progress.update(true);
        } catch (error) {
            const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
            parentPort.postMessage({ 
                type: 'error',
                data: error.message,
                success: false,
                processTime: totalTime
            });
            progress.update(false);
        }
        task.data = null;
    }
});

// Handle unexpected errors
process.on('uncaughtException', (error) => {
    console.error(`Uncaught Exception: ${error.message}`);
    cleanup();
    process.exit(1);
});

// Handle unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    cleanup();
    process.exit(1);
});

// Send initial ready status
parentPort.postMessage({
    type: 'status',
    data: `Worker ${workerData.workerIndex} ready`
});
