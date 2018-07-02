#!/usr/bin/env node
const resolveUrl = require('url').resolve
const request = require('request-promise-native')
const amqplib = require('amqplib')
const Lock = require('semaphore-async-await').Lock
const EventEmitter = require('events')

function showHelp(){
    console.error(`
Creates and maintains a rabbit exchange, 'qps_exchange', which when published to enforces a QPS across queues using parameters provided via rabbit message headers.
Publish your messages to 'qps_exchange' instead of the default exchange supplying a 'qps-key' header and a 'qps-delay' header.
The 'qps-key' is used to serialize all messages sent to the 'qps_exchange' regardless of their routing key which will be preserved
The 'qps-delay' header (in milliseconds) delays after routeing the message using the default exchange. This can be used to approximate a desired QPS.
You may also optionally provide a 'qps-max-priority' to add a priority to the serialization and then use rabbit priority as normal

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

/**
 * Returns all the queues for a rabbit server using the management http interface
 * @param {string} managementConnString
 * @param {string} filterRegex If supplied queues will first be filtered by name
 */
async function listQueues(managementConnString, filterRegex){
    filterRegex = filterRegex || ".*"
    if(typeof(filterRegex)=="string") filterRegex = new RegExp(filterRegex)
    var url = resolveUrl(managementConnString, `/api/queues?lengths_age=1800&lengths_incr=60`) // stats over the last 30min with 30sec samples. See https://rawcdn.githack.com/rabbitmq/rabbitmq-management/v3.7.6/priv/www/doc/stats.html
    var queues = await request({url:url, json:true})
    return queues.filter(queue=>filterRegex.test(queue.name))
}
module.exports.listQueues = listQueues

/** Given a message this function parses all relevant headers  */
function parseHeaders(msg){
    var qpsMaxPriority = parseInt(msg.properties.headers['qps-max-priority'])
    qpsMaxPriority = isNaN(qpsMaxPriority) ? undefined : qpsMaxPriority
    return {
        qpsKey: msg.properties.headers['qps-key'] || null,
        qpsDelay: msg.properties.headers['qps-delay'] || 1000,
        qpsMaxPriority: qpsMaxPriority,
    }
}

/** Utility function to sleep for a given number of milliseconds */
function sleep(milliseconds){
    return new Promise((resolve)=>setTimeout(resolve, milliseconds))
}
module.exports.sleep = sleep

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
    }
    async _forwardMessage(msg, exchange){
        await this.ch.publish(exchange||'', msg.fields.routingKey, msg.content, {
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
        await this.ch.assertQueue('qps_unknown_queue', {durable:true, maxLength:100000})
        await this.ch.bindQueue('qps_unknown_queue', 'qps_unknown_exchange', '#') //routes all messages to the 'qps_unknown_queue'
        await this.ch.assertExchange('qps_exchange', 'headers', {durable:true, alternateExchange:'qps_unknown_exchange'}) //The first time we see a qps-key it is routed to the QPS_UNKNOWN_EXCHANGE via alternateExchange
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
            var {qpsKey, qpsMaxPriority} = parseHeaders(msg)
            if(!qpsKey){
                this.ch.nack(msg, false, false) //did not set a qpsKey which is required for this exchange
                return
            }
            //ensure queue and binding to handle future messages
            this.emit("assertQueue", `qps_key_${qpsKey}`)
            await this.ch.assertQueue(`qps_key_${qpsKey}`, {maxPriority:qpsMaxPriority, durable:false, maxLength:100000}) //XXX: messages with the same qpsKey and diff maxPriority WILL bork the channel
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
        if(this._consumerTags[`qps_key_${qpsKey}`]) return this._consumerTags[`qps_key_${qpsKey}`]
        var lock = new Lock()
        async function consume(msg){
            if(msg == null){
                //rabbit has canceled us http://www.rabbitmq.com/consumer-cancel.html
                delete this._consumerTags[`qps_key_${qpsKey}`]
                return
            }
            var {qpsDelay} = parseHeaders(msg)
            await lock.acquire()
            try {
                //XXX: forwarding to a queue that does not exist borks the channel!
                await this._forwardMessage(msg) //send the message to the default exchange / intended queue
                await this.ch.ack(msg)
                await sleep(qpsDelay)
                lock.release()
            } catch(err){
                lock.release()
                throw err
            }
        }
        consume = consume.bind(this)
        this.emit('consume', `qps_key_${qpsKey}`)
        var consumerTagPromise = this.ch.consume(`qps_key_${qpsKey}`, consume).then(r=>r.consumerTag)
        this._consumerTags[`qps_key_${qpsKey}`] = consumerTagPromise //NOTE: We can not yield before we set this key or we will create multiple consumers
        await this.ch.bindQueue(`qps_key_${qpsKey}`, 'qps_exchange', '', {'qps-key':qpsKey})
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
        var queues = await listQueues(this.managementConnString, '^qps_key_')
        await Promise.all(queues.map(q=>q.name.replace(/^qps_key_/i, '')).map(this.consumeQpsQueue.bind(this)))
    }
    /**
     * Delete any queues that have not been used recently
     * @param {*} maxIdleDuration If a queue has been for this duration (in milliseconds) it will be deleted. The default is 1hr
     */
    async deleteIdleQueues(){
        var idleQueues = (await listQueues(this.managementConnString, '^qps_key_'))
            .filter(q=>{
                try{
                    if(!q['message_stats']['publish_details']['rate']) return true
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
        !argv.test && process.exit(0)
    }
    if(command == "clean" || command == "start"){
        log("cleaning up idle queues...")
        await exchange.deleteIdleQueues()
        !argv.test && process.exit(0)
    }
    if(command == "start"){
        log("consuming existing queues...")
        await exchange.consumeExistingQpsQueues()
        log("consuming new/unknown 'qps-key'...")
        await exchange.consumeQpsUnknownQueue()
        log('scheduling automatic cleanup of idle queues...')
        var cleanIdleQueuesIntervalId = setInterval(exchange.deleteIdleQueues.bind(exchange), 10*60*1000) //every 10min
        log('registering for SIGINT handling...')
        process.on('SIGINT', shutdown.bind(this, exchange, cleanIdleQueuesIntervalId))
        log('success!')
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