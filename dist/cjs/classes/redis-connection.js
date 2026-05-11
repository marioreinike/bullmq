"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisConnection = void 0;
const tslib_1 = require("tslib");
const events_1 = require("events");
const ioredis_1 = require("ioredis");
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
const utils_1 = require("ioredis/built/utils");
const utils_2 = require("../utils");
const version_1 = require("../version");
const scripts = require("../scripts");
const overrideMessage = [
    'BullMQ: WARNING! Your redis options maxRetriesPerRequest must be null',
    'and will be overridden by BullMQ.',
].join(' ');
const deprecationMessage = 'BullMQ: Your redis options maxRetriesPerRequest must be null.';
const clusterReconnectPromise = Symbol('bullmqClusterReconnectPromise');
const clusterPatchedForBlocking = Symbol('bullmqClusterPatchedForBlocking');
const clusterOriginalBzpopmin = Symbol('bullmqClusterOriginalBzpopmin');
const clusterWrappedBzpopmin = Symbol('bullmqClusterWrappedBzpopmin');
const clusterPatchRefCount = Symbol('bullmqClusterPatchRefCount');
const clusterClosingRefCount = Symbol('bullmqClusterClosingRefCount');
const clusterReconnectAttempt = Symbol('bullmqClusterReconnectAttempt');
// Hard cap on a single cluster reconnect attempt. Without this, a hung
// `client.connect()` (e.g. ioredis Cluster cannot recover and slot refresh
// keeps retrying internally) leaves `clusterReconnectPromise` pinned forever
// and every subsequent bzpopmin call awaits the same dead promise, deadlocking
// the worker. 30s is comfortably above the default ioredis slotsRefreshTimeout
// while bounded enough that wedged workers recover within one or two ticks.
const DEFAULT_CLUSTER_RECONNECT_TIMEOUT_MS = 30000;
// Async gap between `disconnect(false)` and `connect()`. `disconnect(false)`
// triggers an asynchronous teardown chain inside ioredis Cluster: each pool
// node's TCP socket closes, the ConnectionPool emits "drain" once `nodes.all`
// is empty, and the Cluster reacts by emitting a "close" event. If
// `connect()` runs synchronously after `disconnect(false)`, it registers its
// own "close" listener while that teardown is still in flight — the listener
// then catches the stale close event from our own disconnect and rejects
// with `"None of startup nodes is available"`, so every reconnect attempt
// fails with the same error and the worker never recovers. Letting the
// event loop turn for ~200ms is empirically enough for the teardown chain
// to settle before the new connect listeners are wired up.
const DISCONNECT_SETTLE_MS = 200;
class RedisConnection extends events_1.EventEmitter {
    constructor(opts, extraOptions) {
        super();
        this.extraOptions = extraOptions;
        this.capabilities = {
            canDoubleTimeout: false,
            canBlockFor1Ms: true,
        };
        this.status = 'initializing';
        this.dbType = 'redis';
        this.packageVersion = version_1.version;
        this.disabledBlockingClusterReconnect = false;
        // Set extra options defaults
        this.extraOptions = Object.assign({ shared: false, blocking: true, skipVersionCheck: false, skipWaitingForReady: false, clusterReconnectTimeoutMs: DEFAULT_CLUSTER_RECONNECT_TIMEOUT_MS }, extraOptions);
        if (!(0, utils_2.isRedisInstance)(opts)) {
            this.checkBlockingOptions(overrideMessage, opts);
            this.opts = Object.assign({ port: 6379, host: '127.0.0.1', retryStrategy: function (times) {
                    return Math.max(Math.min(Math.exp(times), 20000), 1000);
                } }, opts);
            if (this.extraOptions.blocking) {
                this.opts.maxRetriesPerRequest = null;
            }
        }
        else {
            this._client = opts;
            // Test if the redis instance is using keyPrefix
            // and if so, throw an error.
            if (this._client.options.keyPrefix) {
                throw new Error('BullMQ: ioredis does not support ioredis prefixes, use the prefix option instead.');
            }
            if ((0, utils_2.isRedisCluster)(this._client)) {
                this.opts = this._client.options.redisOptions;
            }
            else {
                this.opts = this._client.options;
            }
            this.checkBlockingOptions(deprecationMessage, this.opts, true);
        }
        this.skipVersionCheck =
            (extraOptions === null || extraOptions === void 0 ? void 0 : extraOptions.skipVersionCheck) ||
                !!(this.opts && this.opts.skipVersionCheck);
        this.handleClientError = (err) => {
            this.emit('error', err);
        };
        this.handleClientClose = () => {
            this.emit('close');
        };
        this.handleClientReady = () => {
            this.emit('ready');
        };
        this.initializing = this.init();
        this.initializing.catch(err => this.emit('error', err));
    }
    checkBlockingOptions(msg, options, throwError = false) {
        if (this.extraOptions.blocking && options && options.maxRetriesPerRequest) {
            if (throwError) {
                throw new Error(msg);
            }
            else {
                console.error(msg);
            }
        }
    }
    /**
     * Waits for a redis client to be ready.
     * @param redis - client
     */
    static async waitUntilReady(client) {
        if (client.status === 'ready') {
            return;
        }
        // ioredis Cluster reports 'connect' as its connected status instead of
        // 'ready' that standalone Redis uses. Treat it as already connected so we
        // don't hang waiting for a 'ready' event that will never fire and end up
        // throwing a spurious "Connection is closed" error during disconnect.
        if (client.status === 'connect' && (0, utils_2.isRedisCluster)(client)) {
            return;
        }
        if (client.status === 'wait') {
            return client.connect();
        }
        if (client.status === 'end') {
            throw new Error(utils_1.CONNECTION_CLOSED_ERROR_MSG);
        }
        let handleReady;
        let handleEnd;
        let handleError;
        try {
            await new Promise((resolve, reject) => {
                let lastError;
                handleError = (err) => {
                    lastError = err;
                };
                handleReady = () => {
                    resolve();
                };
                handleEnd = () => {
                    if (client.status !== 'end') {
                        reject(lastError || new Error(utils_1.CONNECTION_CLOSED_ERROR_MSG));
                    }
                    else {
                        if (lastError) {
                            reject(lastError);
                        }
                        else {
                            // when custom 'end' status is set we already closed
                            resolve();
                        }
                    }
                };
                (0, utils_2.increaseMaxListeners)(client, 3);
                client.once('ready', handleReady);
                client.on('end', handleEnd);
                client.once('error', handleError);
            });
        }
        finally {
            client.removeListener('end', handleEnd);
            client.removeListener('error', handleError);
            client.removeListener('ready', handleReady);
            (0, utils_2.decreaseMaxListeners)(client, 3);
        }
    }
    get client() {
        return this.initializing;
    }
    loadCommands(packageVersion, providedScripts) {
        const finalScripts = providedScripts || scripts;
        for (const property in finalScripts) {
            // Only define the command if not already defined
            const commandName = `${finalScripts[property].name}:${packageVersion}`;
            if (!this._client[commandName]) {
                this._client.defineCommand(commandName, {
                    numberOfKeys: finalScripts[property].keys,
                    lua: finalScripts[property].content,
                });
            }
        }
    }
    async init() {
        if (!this._client) {
            const _a = this.opts, { url } = _a, rest = tslib_1.__rest(_a, ["url"]);
            this._client = url ? new ioredis_1.default(url, rest) : new ioredis_1.default(rest);
        }
        (0, utils_2.increaseMaxListeners)(this._client, 3);
        this._client.on('error', this.handleClientError);
        // ioredis treats connection errors as a different event ('close')
        this._client.on('close', this.handleClientClose);
        this._client.on('ready', this.handleClientReady);
        this.patchBlockingClusterClient();
        if (!this.extraOptions.skipWaitingForReady) {
            await RedisConnection.waitUntilReady(this._client);
        }
        this.loadCommands(this.packageVersion);
        if (this._client['status'] !== 'end') {
            const versionResult = await this.getRedisVersionAndType();
            this.version = versionResult.version;
            this.dbType = versionResult.databaseType;
            if (this.skipVersionCheck !== true && !this.closing) {
                if ((0, utils_2.isRedisVersionLowerThan)(this.version, RedisConnection.minimumVersion, this.dbType)) {
                    throw new Error(`Redis version needs to be greater or equal than ${RedisConnection.minimumVersion} ` +
                        `Current: ${this.version}`);
                }
                if ((0, utils_2.isRedisVersionLowerThan)(this.version, RedisConnection.recommendedMinimumVersion, this.dbType)) {
                    console.warn(`It is highly recommended to use a minimum Redis version of ${RedisConnection.recommendedMinimumVersion}
             Current: ${this.version}`);
                }
            }
            this.capabilities = {
                canDoubleTimeout: !(0, utils_2.isRedisVersionLowerThan)(this.version, '6.0.0', this.dbType),
                canBlockFor1Ms: !(0, utils_2.isRedisVersionLowerThan)(this.version, '7.0.8', this.dbType),
            };
            this.status = 'ready';
        }
        return this._client;
    }
    patchBlockingClusterClient() {
        var _a;
        const client = this._client;
        const blockingClient = client;
        if (!this.extraOptions.blocking ||
            !(0, utils_2.isRedisCluster)(client) ||
            typeof blockingClient.bzpopmin !== 'function') {
            return;
        }
        blockingClient[clusterPatchRefCount] =
            (blockingClient[clusterPatchRefCount] || 0) + 1;
        this.patchedBlockingClusterClient = blockingClient;
        if (blockingClient[clusterPatchedForBlocking]) {
            return;
        }
        const reconnectTimeoutMs = (_a = this.extraOptions.clusterReconnectTimeoutMs) !== null && _a !== void 0 ? _a : DEFAULT_CLUSTER_RECONNECT_TIMEOUT_MS;
        const bzpopmin = blockingClient.bzpopmin;
        const wrappedBzpopmin = async (...args) => {
            await RedisConnection.reconnectClusterIfNeeded(blockingClient, reconnectTimeoutMs);
            try {
                return await bzpopmin.apply(blockingClient, args);
            }
            catch (error) {
                const commandError = error;
                if (RedisConnection.shouldReconnectClusterAfterError(blockingClient, commandError)) {
                    try {
                        await RedisConnection.reconnectCluster(blockingClient, reconnectTimeoutMs);
                    }
                    catch (_a) {
                        // Preserve the original command failure if best-effort recovery fails.
                    }
                }
                throw commandError;
            }
        };
        blockingClient[clusterOriginalBzpopmin] = bzpopmin;
        blockingClient[clusterWrappedBzpopmin] = wrappedBzpopmin;
        blockingClient[clusterPatchedForBlocking] = true;
        blockingClient.bzpopmin = wrappedBzpopmin;
    }
    disableBlockingClusterReconnect() {
        const client = this.patchedBlockingClusterClient;
        if (!client || this.disabledBlockingClusterReconnect) {
            return;
        }
        client[clusterClosingRefCount] = (client[clusterClosingRefCount] || 0) + 1;
        this.disabledBlockingClusterReconnect = true;
    }
    releaseBlockingClusterClientPatch() {
        const client = this.patchedBlockingClusterClient;
        if (!client) {
            return;
        }
        if (this.disabledBlockingClusterReconnect) {
            const closingRefCount = (client[clusterClosingRefCount] || 1) - 1;
            if (closingRefCount > 0) {
                client[clusterClosingRefCount] = closingRefCount;
            }
            else {
                delete client[clusterClosingRefCount];
            }
            this.disabledBlockingClusterReconnect = false;
        }
        const patchRefCount = (client[clusterPatchRefCount] || 1) - 1;
        if (patchRefCount > 0) {
            client[clusterPatchRefCount] = patchRefCount;
            this.patchedBlockingClusterClient = undefined;
            return;
        }
        if (client[clusterOriginalBzpopmin] &&
            client.bzpopmin === client[clusterWrappedBzpopmin]) {
            client.bzpopmin = client[clusterOriginalBzpopmin];
        }
        delete client[clusterPatchRefCount];
        delete client[clusterClosingRefCount];
        delete client[clusterOriginalBzpopmin];
        delete client[clusterWrappedBzpopmin];
        delete client[clusterPatchedForBlocking];
        this.patchedBlockingClusterClient = undefined;
    }
    static isClusterWithEmptyNodes(client) {
        return typeof client.nodes === 'function' && client.nodes().length === 0;
    }
    static isReconnectingDisabled(client) {
        const patchRefCount = client[clusterPatchRefCount] || 0;
        const closingRefCount = client[clusterClosingRefCount] || 0;
        return (patchRefCount === 0 ||
            closingRefCount >= patchRefCount ||
            client.status === 'end' ||
            client.status === 'closing');
    }
    static async reconnectClusterIfNeeded(client, timeoutMs) {
        if (!RedisConnection.isReconnectingDisabled(client) &&
            RedisConnection.isClusterWithEmptyNodes(client)) {
            await RedisConnection.reconnectCluster(client, timeoutMs);
        }
    }
    static shouldReconnectClusterAfterError(client, error) {
        var _a, _b;
        if (RedisConnection.isReconnectingDisabled(client)) {
            return false;
        }
        const message = [
            error.message,
            (_a = error.cause) === null || _a === void 0 ? void 0 : _a.message,
            (_b = error.lastNodeError) === null || _b === void 0 ? void 0 : _b.message,
        ].join(' ');
        return (RedisConnection.isClusterWithEmptyNodes(client) ||
            /Command timed out|Failed to refresh slots cache/i.test(message));
    }
    static async reconnectCluster(client, timeoutMs) {
        if (RedisConnection.isReconnectingDisabled(client)) {
            return;
        }
        if (!client[clusterReconnectPromise]) {
            client[clusterReconnectPromise] =
                RedisConnection.connectClusterWithTimeout(client, timeoutMs).finally(() => {
                    client[clusterReconnectPromise] = null;
                });
        }
        await client[clusterReconnectPromise];
    }
    // Disconnects and reconnects a cluster client, racing connect() against a
    // hard cap so a hung reconnect cannot pin `clusterReconnectPromise`
    // indefinitely. On timeout, the underlying connect() is left running
    // (ioredis Cluster handles its own retry/state); we simply stop awaiting it.
    // The next bzpopmin call's `reconnectClusterIfNeeded` check will trigger a
    // fresh reconnect if the pool is still empty.
    //
    // A `bullmq:cluster-reconnect` event is emitted on the underlying ioredis
    // Cluster client with `{ outcome, attempt, durationMs, error? }` so that
    // subscribers (observability layers that already hold a reference to the
    // blocking client) can record reconnect telemetry without bullmq plumbing
    // a tracer through static methods.
    static async connectClusterWithTimeout(client, timeoutMs) {
        var _a, _b;
        const attempt = ((_a = client[clusterReconnectAttempt]) !== null && _a !== void 0 ? _a : 0) + 1;
        client[clusterReconnectAttempt] = attempt;
        const startedAt = Date.now();
        let outcome = 'success';
        let caught;
        try {
            // `disconnect(true)` is ioredis's "I'm about to reconnect" signal — it
            // drains the connection pool without setting `manuallyClosing=true`.
            // The `false` variant (used by the original 5.76.6 patch) flips
            // `manuallyClosing` to true, which poisons `handleCloseEvent` into the
            // terminal `status="end"` branch when our subsequent `connect()`
            // rejects. Once the cluster lands in `"end"`, ioredis will not revive
            // it on its own (no `clusterRetryStrategy` runs from there) and our
            // worker's bzpopmin loop never recovers — `isReconnectingDisabled`
            // correctly treats `"end"` as terminal (genuine shutdown and
            // user-configured `clusterRetryStrategy` give-up both legitimately
            // land there). Using `disconnect(true)` keeps `manuallyClosing`
            // unchanged so a failed reconnect transitions through `"reconnecting"`
            // instead, leaving both ioredis and this worker able to retry.
            client.disconnect(true);
            // Let the disconnect's asynchronous teardown chain settle before
            // connect() registers its listeners. See DISCONNECT_SETTLE_MS comment
            // for the failure mode this prevents.
            await new Promise(resolve => {
                var _a;
                const handle = setTimeout(resolve, DISCONNECT_SETTLE_MS);
                (_a = handle.unref) === null || _a === void 0 ? void 0 : _a.call(handle);
            });
            let timeoutHandle;
            try {
                await Promise.race([
                    client.connect(),
                    new Promise((_, reject) => {
                        var _a;
                        timeoutHandle = setTimeout(() => {
                            outcome = 'timeout';
                            reject(new Error(`BullMQ: cluster reconnect timed out after ${timeoutMs}ms`));
                        }, timeoutMs);
                        // Don't keep the event loop alive solely for this timer.
                        (_a = timeoutHandle.unref) === null || _a === void 0 ? void 0 : _a.call(timeoutHandle);
                    }),
                ]);
            }
            finally {
                if (timeoutHandle) {
                    clearTimeout(timeoutHandle);
                }
            }
        }
        catch (err) {
            caught = err;
            if (outcome === 'success') {
                outcome = 'error';
            }
            throw caught;
        }
        finally {
            const event = Object.assign({ outcome,
                attempt, durationMs: Date.now() - startedAt }, (caught ? { error: caught.message } : {}));
            // A subscriber throwing here must not break the reconnect contract.
            try {
                (_b = client.emit) === null || _b === void 0 ? void 0 : _b.call(client, 'bullmq:cluster-reconnect', event);
            }
            catch (_c) {
                // ignore listener errors
            }
        }
    }
    async disconnect(wait = true) {
        const client = await this.client;
        if (client.status !== 'end') {
            let _resolve, _reject;
            if (!wait) {
                return client.disconnect();
            }
            const disconnecting = new Promise((resolve, reject) => {
                (0, utils_2.increaseMaxListeners)(client, 2);
                client.once('end', resolve);
                client.once('error', reject);
                _resolve = resolve;
                _reject = reject;
            });
            client.disconnect();
            try {
                await disconnecting;
            }
            finally {
                (0, utils_2.decreaseMaxListeners)(client, 2);
                client.removeListener('end', _resolve);
                client.removeListener('error', _reject);
            }
        }
    }
    async reconnect() {
        const client = await this.client;
        return client.connect();
    }
    async close(force = false) {
        if (!this.closing) {
            const status = this.status;
            this.status = 'closing';
            this.closing = true;
            this.disableBlockingClusterReconnect();
            try {
                if (status === 'ready') {
                    // Not sure if we need to wait for this
                    await this.initializing;
                }
                if (!this.extraOptions.shared) {
                    if (status == 'initializing' || force) {
                        // If we have not still connected to Redis, we need to disconnect.
                        this._client.disconnect();
                    }
                    else {
                        await this._client.quit();
                    }
                    // As IORedis does not update this status properly, we do it ourselves.
                    this._client['status'] = 'end';
                }
            }
            catch (error) {
                if ((0, utils_2.isNotConnectionError)(error)) {
                    throw error;
                }
            }
            finally {
                this.releaseBlockingClusterClientPatch();
                this._client.off('error', this.handleClientError);
                this._client.off('close', this.handleClientClose);
                this._client.off('ready', this.handleClientReady);
                (0, utils_2.decreaseMaxListeners)(this._client, 3);
                this.removeAllListeners();
                this.status = 'closed';
            }
        }
    }
    async getRedisVersionAndType() {
        if (this.skipVersionCheck) {
            return {
                version: RedisConnection.minimumVersion,
                databaseType: 'redis',
            };
        }
        const doc = await this._client.info();
        const redisPrefix = 'redis_version:';
        const maxMemoryPolicyPrefix = 'maxmemory_policy:';
        const lines = doc.split(/\r?\n/);
        let redisVersion;
        let databaseType = 'redis';
        // Detect database type from server info
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            // Check for Dragonfly
            if (line.includes('dragonfly_version:') ||
                line.includes('server:Dragonfly')) {
                databaseType = 'dragonfly';
                // For Dragonfly, extract version from dragonfly_version field
                if (line.indexOf('dragonfly_version:') === 0) {
                    redisVersion = line.substr('dragonfly_version:'.length);
                }
            }
            // Check for Valkey
            else if (line.includes('valkey_version:') ||
                line.includes('server:Valkey')) {
                databaseType = 'valkey';
                // For Valkey, extract version from valkey_version field
                if (line.indexOf('valkey_version:') === 0) {
                    redisVersion = line.substr('valkey_version:'.length);
                }
            }
            // Standard Redis version detection
            else if (line.indexOf(redisPrefix) === 0) {
                redisVersion = line.substr(redisPrefix.length);
                // Keep Redis as default unless we find evidence of other databases above
                if (databaseType === 'redis') {
                    databaseType = 'redis';
                }
            }
            if (line.indexOf(maxMemoryPolicyPrefix) === 0) {
                const maxMemoryPolicy = line.substr(maxMemoryPolicyPrefix.length);
                if (maxMemoryPolicy !== 'noeviction') {
                    console.warn(`IMPORTANT! Eviction policy is ${maxMemoryPolicy}. It should be "noeviction"`);
                }
            }
        }
        // Fallback version detection if specific database version field wasn't found
        if (!redisVersion) {
            // Try to find any version field as fallback
            for (const line of lines) {
                if (line.includes('version:')) {
                    const parts = line.split(':');
                    if (parts.length >= 2) {
                        redisVersion = parts[1];
                        break;
                    }
                }
            }
        }
        return {
            version: redisVersion || RedisConnection.minimumVersion,
            databaseType,
        };
    }
    get redisVersion() {
        return this.version;
    }
    get databaseType() {
        return this.dbType;
    }
}
exports.RedisConnection = RedisConnection;
RedisConnection.minimumVersion = '5.0.0';
RedisConnection.recommendedMinimumVersion = '6.2.0';
//# sourceMappingURL=redis-connection.js.map