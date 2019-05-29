#!/usr/bin/env node
"use strict";
const resolveUrl = require('url').resolve
const request = require('request-promise-native')
const amqplib = require('amqplib')
const Lock = require('semaphore-async-await').Lock
const EventEmitter = require('events')
const NEWLINE_BUFFER = Buffer.from('\n')

function showHelp(){
    console.error(`
Creates and maintains a rabbit exchange, 'qps_exchange', which when published to enforces a QPS across queues using parameters provided via rabbit message headers.
See README.md for more usage details

Usage: ./rabbit_qps_shovel.js [OPTIONS] COMMAND

commands:
    help    show this message
    init    create exchange and required queues then exit
    clean   clean up idle queues then exit
    start   ensure exchange and required queues, clean idle queues, and then start processing messages

options:
    --management Rabbit management connection string. Example http://guest:guest@localhost:15672/ (REQUIRED)
    --connection Rabbit connection string. See https://www.rabbitmq.com/uri-spec.html (REQUIRED)
    `.trim())
    process.exit(1)
}

const MAX_LENGTH = 100000
const QUEUE_PREFIX = 'qps_key'

/**
 * Returns all the queues for a rabbit server using the management http interface
 * @param {string} managementConnString
 * @param {string} filterRegex If supplied queues will first be filtered by name
 */
async function listQueues(managementConnString, filterRegex){
    filterRegex = filterRegex || ".*"
    if(typeof(filterRegex)=="string") filterRegex = new RegExp(filterRegex)
    var url = resolveUrl(managementConnString, `/api/queues?lengths_age=900&lengths_incr=60`) // stats over the last 15min with 30sec samples. See https://rawcdn.githack.com/rabbitmq/rabbitmq-management/v3.7.6/priv/www/doc/stats.html
    var queues = await request({url:url, json:true})
    return queues.filter(queue=>filterRegex.test(queue.name))
}
module.exports.listQueues = listQueues

/** Given a message this function parses all relevant headers  */
function parseHeaders(msg){
    var qpsMaxPriority = parseInt(msg.properties.headers['qps-max-priority']) //needs to be an int not a float
    qpsMaxPriority = isNaN(qpsMaxPriority) ? undefined : qpsMaxPriority
    return {
        qpsKey: msg.properties.headers['qps-key'] || null,
        qpsDelay: msg.properties.headers['qps-delay'] || 1000,
        qpsMaxPriority: qpsMaxPriority,
        qpsDurable: !!msg.properties.headers['qps-durable'],
        qpsBatchSize: msg.properties.headers['qps-batch-size'] || 1,
        qpsBatchTimeout: msg.properties.headers['qps-batch-timeout'] || 1000,
        qpsBatchCombine: msg.properties.headers['qps-batch-combine'] || false,
        qpsDelayLockKey: msg.properties.headers['qps-delay-lock-key'] || null,
        qpsCubicMaxQps: msg.properties.headers['qps-cubic-max-qps'] || null,
        qpsCubicMaxQpsTime: msg.properties.headers['qps-cubic-max-qps-time'] || 10*60*1000, //default to 10 min
    }
}

function joinBuffersWithNewLines(buffers){
    var content = []
    buffers.forEach(msg=>{
        content.push(msg)
        content.push(NEWLINE_BUFFER)
    })
    content.pop() //remove trailing newline
    return Buffer.concat(content)
}

/** Utility function to sleep for a given number of milliseconds */
function sleep(milliseconds){
    return new Promise((resolve)=>setTimeout(resolve, milliseconds))
}
module.exports.sleep = sleep

//interface IExchangeConsumer { consume(msg:RabbitMessage):Promise<void> }
const CUBIC_MULTIPLICATION_DECREASE_FACTOR = .5 //On error half the load
module.exports.CUBIC_MULTIPLICATION_DECREASE_FACTOR = CUBIC_MULTIPLICATION_DECREASE_FACTOR
class CubicExchangeConsumer /* implements IExchangeConsumer */ {
    //See http://squidarth.com/rc/programming/networking/2018/07/18/intro-congestion.html & https://en.wikipedia.org/wiki/CUBIC_TCP for more info about this class
    constructor(qpsExchange){
        this.qpsExchange = qpsExchange
        this.qps = 0 // current qps aka cwnd aka congestion window (in orig alg)
        this.time = 1 //time in MS elapsed since the last window reduction
        this.lastQps = 0 //qps just before last reduction aka wmax (in orig alg)
        this.qpsKey = null
        this.qpsExchange.on('nack', this.onNack.bind(this))
        this.scalingFactor = null//cubic scaling factor
    }
    onNack(msg){
        if(time < 1000) return //ignore nacks in short succession
        if(parseHeaders(msg).qpsKey !== this.qpsKey) return
        this.reduction()
    }
    tickTime(maxQps, maxQpsTime){
        //This function uses the formula from the wikipedia page, https://en.wikipedia.org/wiki/CUBIC_TCP , with a few tweaks
        if(maxQps && maxQpsTime){
            this.scalingFactor = maxQps/Math.pow(maxQpsTime/1000,3.0) //tweak the cubic scaling factor depending on how fast we want to scale up
        }
        //start of actual formula
        var wmax = this.lastQps*10 //qps/cwnd really has to be bigger than 1 so we scale by 10 so we can do .1 qps (*10 in formula)
        var K = Math.cbrt(CUBIC_MULTIPLICATION_DECREASE_FACTOR*wmax/this.scalingFactor)
        this.qps = this.scalingFactor*Math.pow((this.time/1000)-K, 3) + wmax //Constants were tweaked using seconds (hence we convert to seconds in formula)
        if(this.qps < .1) this.qps = 0.1 //min usable in formula
        else if(maxQps && this.qps>maxQps) this.qps = maxQps //enforce max qps option
        this.time += Math.max(1000.0/this.qps,1) //we don't use real time so that we can handle spiky traffic
    }
    reduction(){
        this.lastQps = this.qps
        this.time = 1
        this.tickTime()
        this.time = 1
    }
    async consume(msg) {
        var headers = parseHeaders(msg)
        var {qpsCubicMaxQps, qpsDelayLockKey, qpsKey, qpsCubicMaxQpsTime} = headers
        this.qpsKey = this.qpsKey || qpsKey
        qpsDelayLockKey = qpsDelayLockKey || qpsKey
        var sleepLock = this.qpsExchange.globalSleepLocks[qpsDelayLockKey] || (this.qpsExchange.globalSleepLocks[qpsDelayLockKey] = new Lock())
        await sleepLock.acquire()
        try {
            this.tickTime(qpsCubicMaxQps, qpsCubicMaxQpsTime)
            msg.properties.headers['qps-cubic-current-qps'] = this.qps
            await this.qpsExchange._forwardMessage(msg)
            await sleep(1000/this.qps)
        } finally {
            sleepLock.release()
        }
    }
}
module.exports.CubicExchangeConsumer = CubicExchangeConsumer

class QpsExchangeConsumer /* implements IExchangeConsumer */ {
    constructor(qpsExchange){
        this.qpsExchange = qpsExchange
        this.batchList = []
        this.batchStartTimeoutId = null
    }
    async send({qpsBatchCombine, qpsDelay}) {
        var batchList = this.batchList
        if(batchList.length == 0) return
        //XXX: forwarding to a queue that does not exist borks the channel!
        //send the message to the default exchange / intended queue
        if(qpsBatchCombine && batchList.length > 1){
            var content = joinBuffersWithNewLines(batchList.map(msg=>msg.content))
            await this.qpsExchange._forwardMessage(batchList[0], null, content)
        } else {
            await Promise.all(batchList.map(msg=>this.qpsExchange._forwardMessage(msg)))
        }
        await Promise.all(batchList.map(msg=>this.qpsExchange.ch.ack(msg)))
        await sleep(qpsDelay)
        batchList.splice(0) //clear list
        clearTimeout(this.batchStartTimeoutId) //clear timeout so we don't resend the batch
        this.batchStartTimeoutId = null
    }
    async consume(msg) {
        var headers = parseHeaders(msg)
        var {qpsBatchSize, qpsBatchTimeout, qpsDelayLockKey, qpsKey} = headers
        qpsDelayLockKey = qpsDelayLockKey || qpsKey
        var sleepLock = this.qpsExchange.globalSleepLocks[qpsDelayLockKey] || (this.qpsExchange.globalSleepLocks[qpsDelayLockKey] = new Lock())

        await sleepLock.acquire()
        try {
            if(msg == null){
                //rabbit has canceled us http://www.rabbitmq.com/consumer-cancel.html
                delete this.qpsExchange._consumerTags[`${QUEUE_PREFIX}_${qpsKey}`]
                return
            }
            await this.qpsExchange.setPrefetch(qpsBatchSize) //make sure we can handle the batch size
            this.batchList.push(msg)

            // Send the batch if it is full
            if(this.batchList.length >= qpsBatchSize) {
                await this.send(headers)
            }

            // After X seconds, send the batch even if it isn't full
            else if(this.batchStartTimeoutId === null){
                this.batchStartTimeoutId = setTimeout(async ()=>{
                    await sleepLock.acquire()
                    try {
                        await this.send(headers)
                    }
                    finally {
                        sleepLock.release()
                    }
                }, qpsBatchTimeout)
            }
        }
        finally {
            sleepLock.release()
        }
    }
}

class QpsExchange extends EventEmitter {
    constructor({rabbitConnString, managementConnString}){
        super()
        //settings
        this.rabbitConnString = rabbitConnString
        this.managementConnString = managementConnString
        //state
        this._consumerTags = {} //map from queue name to consumer tag promise. Used to ensure there is only ever one consumer for a queue
        this.conn = null //rabbit connection
        this.ch = null //rabbit channel
        this.prefetch = 1
        this.globalSleepLocks = {} //map from lock name (often qps-key) to the lock that should be acquired to send msg
    }
    async _forwardMessage(msg, exchange, content){
        await this.ch.publish(exchange||'', msg.fields.routingKey, content||msg.content, {
            headers: msg.properties.headers,
            priority: msg.priority
        })
    }
    /**
     * Connects to rabbit
     */
    async connect(){
        this.conn = await amqplib.connect(this.rabbitConnString)
        this.ch = await this.conn.createChannel()
        this.ch.on("error", function(err) {
            console.error("[AMQP] channel error", err.message)
        })
        this.ch.on("close", function() {
            console.error("[AMQP] channel closed")
        })
        await this.setPrefetch(10)
    }

    async setPrefetch(prefetch){
        if(prefetch <= this.prefetch) return
        this.prefetch = prefetch
        return await this.ch.prefetch(this.prefetch)
    }

    /**
     * Inits exchanges and required queues
     */
    async initExchanges(){
        /*
            Exchange logic is
            1. You publish your message to `qps_exchange` instead of the default exchange supplying a `qps-key` header and a `qps-delay` header
                `qps-key` serializes all msgs sent to the `qps_exchange` and delays `qps-delay` (in milliseconds) after routeing then using the default exchange
            2. If your `qps-key` has been seen before a binding in rabbit will automagically put your msg in the correct qps queue
            3. Else, your message ends up the QPS_UNKNOWN_QUEUE and a consumer creates a binding for future messages and forwards the msg to the correct qps queue
            4. A consumer ensures your desired qps is enforced (by looking at the `qps-delay` header) before your message is sent to the default exchange
        */
        await this.ch.assertExchange('qps_unknown_exchange', 'topic', {durable:true})
        this.emit("assertQueue", `qps_unknown_queue`)
        await this.ch.assertQueue('qps_unknown_queue', {durable:true, maxLength:MAX_LENGTH})
        await this.ch.bindQueue('qps_unknown_queue', 'qps_unknown_exchange', '#') //routes all messages to the 'qps_unknown_queue'
        await this.ch.assertExchange('qps_exchange', 'headers', {durable:true, alternateExchange:'qps_unknown_exchange'}) //The first time we see a qps-key it is routed to the QPS_UNKNOWN_EXCHANGE via alternateExchange
    }
    async initAndConsumeNackQueue(){
        if(this._consumerTags['qps_nack_queue']) return this._consumerTags['qps_nack_queue']
        await this.ch.assertQueue('qps_nack_queue', {maxLength:MAX_LENGTH})
        this._consumerTags['qps_nack_queue'] = this.ch.consume('qps_nack_queue',  this.emit.bind(this, 'nack')).then(r=>r.consumerTag)
        return this._consumerTags['qps_nack_queue']
    }
    /**
     * Start a consumer for any unknown 'qps-key'
     */
    async consumeQpsUnknownQueue(){
        if(this._consumerTags['qps_unknown_queue']) return this._consumerTags['qps_unknown_queue']
        async function consume(msg) {
            if(msg == null){
                //rabbit has canceled us http://www.rabbitmq.com/consumer-cancel.html
                delete this._consumerTags['qps_unknown_queue']
                return
            }
            var headers = parseHeaders(msg)
            var {qpsKey, qpsMaxPriority, qpsDurable} = headers
            if(!qpsKey){
                this.ch.nack(msg, false, false) //did not set a qpsKey which is required for this exchange
                return
            }
            //ensure queue and binding to handle future messages
            this.emit("assertQueue", `${QUEUE_PREFIX}_${qpsKey}`)
            let args = {
                maxPriority:qpsMaxPriority,
                durable:qpsDurable,
                maxLength:MAX_LENGTH
            }
            await this.ch.assertQueue(`${QUEUE_PREFIX}_${qpsKey}`, args) //XXX: messages with the same qpsKey and diff maxPriority WILL bork the channel
            await this.consumeQpsQueue(qpsKey) //start a consumer of the queue if there is not one
            //forward the message back through the qps_exchange which should hit the binding we just created
            await this._forwardMessage(msg, 'qps_exchange')
            await this.ch.ack(msg)
        }
        consume = consume.bind(this)
        var consumerTagPromise = this.ch.consume('qps_unknown_queue', consume).then(r=>r.consumerTag) //NOTE: We can not yield before we set this key or we will create multiple consumers
        this._consumerTags['qps_unknown_queue'] = consumerTagPromise
        return await consumerTagPromise //can be used to cancel the consume
    }
    /**
     * Starts a consumer that shovels all message for a given `qps-key`. The consumer will respect the `qps-delay` header
     * @param {*} qpsKey The `qps-key` to shovel messages for
     */
    async consumeQpsQueue(qpsKey){
        if(this._consumerTags[`${QUEUE_PREFIX}_${qpsKey}`]) return this._consumerTags[`${QUEUE_PREFIX}_${qpsKey}`]
        this.emit('consume', `${QUEUE_PREFIX}_${qpsKey}`)
        var consumer;
        var consumerTagPromise = this.ch.consume(`${QUEUE_PREFIX}_${qpsKey}`, async (msg)=>{
            if(consumer) return consumer.consume(msg)
            //based on the qpsKey alone we don't know what type of consumer to create until we get a message
            var headers = parseHeaders(msg)
            if(headers.qpsCubicMaxQps){
                consumer = new CubicExchangeConsumer(this)
                await this.initAndConsumeNackQueue()
            } else {
                consumer = new QpsExchangeConsumer(this)
            }
            return consumer.consume(msg)
        }).then(r=>r.consumerTag)
        this._consumerTags[`${QUEUE_PREFIX}_${qpsKey}`] = consumerTagPromise //NOTE: We can not yield before we set this key or we will create multiple consumers
        await this.ch.bindQueue(`${QUEUE_PREFIX}_${qpsKey}`, 'qps_exchange', '', {'qps-key':qpsKey})
        return await consumerTagPromise //can be used to cancel the consume
    }
    /**
     * Cancels all consumers in preparation for shutdown
     */
    async cancelAllConsumers(){
        await Promise.all(Object.keys(this._consumerTags)
            .map(this.cancelConsumer.bind(this)))
    }
    /**
     * Cancels the consumer for a given queue iff there is one
     * @param {*} queue The queue to stop consuming
     */
    async cancelConsumer(queue){
        if(!this._consumerTags[queue]) return
        await this.ch.cancel(await this._consumerTags[queue])
        delete this._consumerTags[queue]
    }
    /**
     * Uses the management interface to discover and existing `qps-key` queues and starts a consumer for them
     */
    async consumeExistingQpsQueues(){
        var queues = await listQueues(this.managementConnString, `^${QUEUE_PREFIX}_`)
        await Promise.all(queues.map(q=>q.name.replace(new RegExp(`${QUEUE_PREFIX}_`), '')).map(this.consumeQpsQueue.bind(this)))
    }
    /**
     * Delete any queues that have not been used recently
     */
    async deleteIdleQueues(){
        var idleQueues = (await listQueues(this.managementConnString, `^${QUEUE_PREFIX}_`))
            .filter(q=>{
                try{
                    return !q['message_stats']['publish_details']['rate'] && !q['messages']
                } catch(err){}
            })
            .map(q=>q.name)
        idleQueues.map(q=>this.emit("deleteQueue", q))
        await Promise.all(idleQueues.map(q=>this.cancelConsumer(q)))
        await Promise.all(idleQueues.map(q=>this.ch.deleteQueue(q)))
    }
}
module.exports.QpsExchange = QpsExchange

async function shutdown(exchange, cleanIdleQueuesIntervalId){
    var log = console.error
    log('detected SIGINT shutting down cleanly...')
    log('stopping cleanup of idle queues...')
    clearInterval(cleanIdleQueuesIntervalId)
    log('canceling consumers...')
    await exchange.cancelAllConsumers()
    log('closing connection...')
    await exchange.conn.close()
    await sleep(3000)
    process.exit(0)
}

async function main(argv){
    //parse / validate cmd line
    var log = console.error
    var command = ((argv.argv||[])[0] || 'help').toLowerCase()
    if(argv.h || argv.help || command=="help") showHelp()
    if(!argv.connection){ log('Error: --connection is required\n'); showHelp() }
    if(!argv.management){ log('Error: --management is required\n'); showHelp() }
    if(['init','start','clean'].indexOf(command)==-1){ showHelp() }

    //connect and stuff
    var exchange = new QpsExchange({
        rabbitConnString: argv.connection,
        managementConnString: argv.management,
    })
    exchange.on('consume', log.bind(null, 'consume'))
    exchange.on('deleteQueue', log.bind(null, 'deleteQueue'))
    exchange.on('assertQueue', log.bind(null, 'assertQueue'))
    log('connecting to rabbit...')
    await exchange.connect()
    function handleRabbitError(){
        log('rabbit error: '+JSON.stringify(Array.from(arguments)))
        !argv.test && process.exit(1)
    }
    exchange.conn.on('close', handleRabbitError.bind(null, 'connection close'))
    exchange.conn.on('error', handleRabbitError.bind(null, 'connection error'))
    exchange.ch.on('close', handleRabbitError.bind(null, 'channel close'))
    exchange.ch.on('error', handleRabbitError.bind(null, 'channel error'))

    //run the actual command
    if(command == "init" || command == "start"){
        log('initializing required exchanges and queues...')
        await exchange.initExchanges()
    }
    if(command == "clean" || command == "start"){
        log("cleaning up idle queues...")
        await exchange.deleteIdleQueues()
    }
    if(command == "start"){
        log("consuming existing queues...")
        await exchange.consumeExistingQpsQueues()
        log("consuming new/unknown 'qps-key'...")
        await exchange.consumeQpsUnknownQueue()
        log('scheduling automatic cleanup of idle queues...')
        var cleanIdleQueuesIntervalId = setInterval(exchange.deleteIdleQueues.bind(exchange), 60*60*1000) //every 60min
        log('registering for SIGINT handling...')
        process.on('SIGINT', shutdown.bind(this, exchange, cleanIdleQueuesIntervalId))
    }
    log('success!')
    if((command == "init" || command == "clean") && !argv.test){
        process.exit(0)
    }
}
module.exports.main = main //only exported for testing

if (require.main === module) {
    main(require('argh').argv)
    /* you can queue a test message with the following commands
    $ ./rabbit_qps_exchange.js --connection "amqp://localhost" --management "http://guest:guest@localhost:15672/" start &
    $ rabbitmqadmin declare queue name=test durable=false
    $ rabbitmqadmin publish exchange=qps_exchange routing_key=test payload="$(date)" properties='{"headers":{"qps-key":"test"}}'
    $ rabbitmqadmin get queue=test requeue=false
    */
}
