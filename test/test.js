var assert = require('assert')
var {QpsExchange, sleep, listQueues, main} = require('../rabbit_qps_exchange')

var rabbitConnString = process.env['rabbitConnString'] || process.env['RABBIT_CONN_STRING'] || "amqp://localhost"
var managementConnString = process.env['managementConnString'] || process.env['MANAGEMENT_CONN_STRING'] || "http://guest:guest@localhost:15672/"

describe('QpsExchange', function() {
    this.timeout(5000);
    var exchange = null

    //init state and cleanup before and after each test
    beforeEach(async function(){
        console.log('starting new test...')
        exchange = new QpsExchange({managementConnString, rabbitConnString})
        await exchange.connect()
    })
    afterEach(async function(){
        //close test's connection
        try{ await exchange.conn.close() } catch(err){}
        exchange = null
        //cleanup stuff that might have been created
        console.log('creating connection for cleanup and cleaning...')
        var e = new QpsExchange({managementConnString, rabbitConnString})
        await e.connect()
        await Promise.all([
            e.ch.deleteQueue('qps_key_test1'),
            e.ch.deleteQueue('qps_key_test2'),
            e.ch.deleteQueue('destination_test1'),
            e.ch.deleteQueue('destination_test2'),
            e.ch.deleteQueue('qps_unknown_queue'),
            e.ch.deleteExchange('qps_unknown_exchange'),
            e.ch.deleteExchange('qps_exchange'),
        ])
        await e.conn.close()
    })

    it('should connect', async function() {
        //await exchange.connect() //done in beforeEach
    })

    it('should init exchanges', async function(){
        await exchange.initExchanges()
        await exchange.ch.checkQueue('qps_unknown_queue')
        await exchange.ch.checkExchange('qps_unknown_exchange')
        await exchange.ch.checkExchange('qps_exchange')
    })

    it('forwardMessage should work', async function(){
        var ch = exchange.ch
        var hasForwarded = false
        await ch.assertQueue(`qps_key_test1`, {durable:false})
        await ch.sendToQueue(`qps_key_test1`, new Buffer('content'), {headers:{'qps-key':'test1'}})
        await new Promise(async function(resolve, reject){
            await ch.consume('qps_key_test1', async function(msg) {
                //implicitly assert routeing key did not change (which has to be the case if this function is called a 2nd time)
                assert.equal(msg.properties.headers['qps-key'], 'test1') //assert headers are preserved
                assert.equal(msg.content.toString(), 'content') //assert content is preserved
                ch.ack(msg)
                if(!hasForwarded){
                    hasForwarded = true //only forward once
                    exchange._forwardMessage(msg, '') //forward msg to default exchange... aka back to this queue
                } else {
                    resolve()
                }
            })
        })
    })

    it('should end up in the correct queue', async function(){
        var ch = exchange.ch
        await ch.assertQueue(`qps_key_test1`, {durable:false})
        await ch.assertQueue('destination_test1', {durable:false})
        //init the exchange
        await exchange.initExchanges()
        await exchange.consumeQpsQueue('test1') //create binding for qps_key_test1 created above
        var publishTime = +(new Date())
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content'), {
            headers:{'qps-key':'test1','qps-delay':'1000'}
        })
        //check to see if message ended up in the correct place
        var msg = await receiveSingleMessage(ch, 'destination_test1')
        var receiveTime = msg.time
        console.info(`it took ${receiveTime - publishTime}ms for the message to arrive`)
        assert.ok(receiveTime - publishTime < 500, 'message arrived too slowly')
        assert.equal(msg.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg.content.toString(), 'content') //assert content is preserved
    })

    it('should create queues for unknown "qps-key"', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content2'), {
            headers:{'qps-key':'test1','qps-delay':'100'}
        })
        await sleep(500) //so there is enough time to create unknown queue
        await ch.checkQueue('qps_key_test1')
        //check to see if message ended up in the correct place
        var msg = await receiveSingleMessage(ch, 'destination_test1')
        assert.equal(msg.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg.content.toString(), 'content2') //assert content is preserved
    })

    //if you use this method you should only expect to receive X messages ever from a queue even if you consume elsewhere... it is a hack for testing that cleans up poorly
    //it also adds a time key to the message which is the time the message was received
    function receiveXMessages(ch, queue, numberOfMessages, timeout){
        timeout = timeout || 2000
        var messages = []
        return new Promise(async function(resolve, reject){
            setTimeout(reject.bind(null, 'message arrived too slow or not at all'), timeout)
            await ch.consume(queue, function(msg){
                msg.time=+(new Date())
                ch.ack(msg)
                messages.push(msg)
                if(messages.length >= numberOfMessages) resolve(messages)
            })
        })
    }
    async function receiveSingleMessage(ch, queue){
        return (await receiveXMessages(ch, queue, 1, 2000))[0]
    }

    it('should timeout and send batch', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('contentbt'), {
            headers:{'qps-key':'test1','qps-delay':'10','qps-batch-size':'2','qps-batch-timeout':100}
        })
        var msg = await receiveSingleMessage(ch, 'destination_test1')
        assert.equal(msg.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg.content.toString(), 'contentbt') //assert content is preserved
    })


    it('should not send messages until batch is full', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content1'), {
            headers:{'qps-key':'test1','qps-delay':'10','qps-batch-size':'2','qps-batch-timeout':10*1000}
        })
        try{
            await receiveXMessages(ch, 'destination_test1', 1, 200)
            assert(false, "Should have failed to send message")
        } catch(err){}
    })

    it('should optionally combine batches into one message', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content1'), {
            headers:{'qps-key':'test1','qps-delay':'10','qps-batch-size':'2','qps-batch-timeout':10*1000, 'qps-batch-combine':true}
        })
        await sleep(100) //sleep to ensure message order cause awaiting a publish does not confirm without rabbit extension/mode
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content2'), {
            headers:{'qps-key':'test1','qps-delay':'10','qps-batch-size':'2','qps-batch-timeout':10*1000, 'qps-batch-combine':true}
        })
        var msg = await receiveSingleMessage(ch, 'destination_test1')
        assert.equal(msg.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg.content.toString(), 'content1\ncontent2') //assert content is preserved
    })

    it('should enforce qps across queues', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await ch.assertQueue('destination_test2', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        //publish
        var publishTime = +(new Date())
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content1'), {
            headers:{'qps-key':'test1','qps-delay':'500'}
        })
        await sleep(100) //sleep to ensure message order cause awaiting a publish does not confirm without rabbit extension/mode
        await ch.publish('qps_exchange', 'destination_test2', new Buffer('content2'), {
            headers:{'qps-key':'test1','qps-delay':'500'}
        })
        //check results
        var [msg1, msg2] = await Promise.all([
            receiveSingleMessage(ch, 'destination_test1'),
            receiveSingleMessage(ch, 'destination_test2'),
        ])

        //this.timeout(60*60*1000)
        //await sleep(60*60*1000)

        assert.equal(msg1.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg1.content.toString(), 'content1') //assert content is preserved
        assert.equal(msg2.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg2.content.toString(), 'content2') //assert content is preserved

        console.info(`it took ${msg1.time - publishTime}ms for msg1 to arrive`)
        console.info(`it took ${msg2.time - publishTime}ms for msg2 to arrive`)
        assert.ok(msg1.time - publishTime < 250, 'message 1 arrived too slowly')
        assert.ok(msg2.time - publishTime > 500, 'message 2 arrived too quickly')
        assert.ok(msg2.time - publishTime < 750, 'message 2 arrived too slowly')
    })

    it('should enforce qps across global lock key', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await ch.assertQueue('destination_test2', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()
        //publish
        var publishTime = +(new Date())
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content1'), {
            headers:{'qps-key':'test1','qps-delay':'500','qps-delay-lock-key':'bob'}
        })
        await sleep(100) //sleep to ensure message order cause awaiting a publish does not confirm without rabbit extension/mode
        await ch.publish('qps_exchange', 'destination_test2', new Buffer('content2'), {
            headers:{'qps-key':'test2','qps-delay':'500','qps-delay-lock-key':'bob'}
        })
        //check results
        var [msg1, msg2] = await Promise.all([
            receiveSingleMessage(ch, 'destination_test1'),
            receiveSingleMessage(ch, 'destination_test2'),
        ])

        //this.timeout(60*60*1000)
        //await sleep(60*60*1000)

        assert.equal(msg1.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg1.content.toString(), 'content1') //assert content is preserved
        assert.equal(msg2.properties.headers['qps-key'], 'test2') //assert headers are preserved
        assert.equal(msg2.content.toString(), 'content2') //assert content is preserved

        console.info(`it took ${msg1.time - publishTime}ms for msg1 to arrive`)
        console.info(`it took ${msg2.time - publishTime}ms for msg2 to arrive`)
        assert.ok(msg1.time - publishTime < 250, 'message 1 arrived too slowly')
        assert.ok(msg2.time - publishTime > 500, 'message 2 arrived too quickly')
        assert.ok(msg2.time - publishTime < 750, 'message 2 arrived too slowly')
    })

    it('should respect message priority', async function(){
        var ch = exchange.ch
        await ch.assertQueue('destination_test1', {durable:false})
        await exchange.initExchanges()
        await exchange.consumeQpsUnknownQueue()

        //publish a message to get things created
        var publishTime = +(new Date())
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content0'), {
            priority:1, headers:{'qps-key':'test1','qps-delay':'10', 'qps-max-priority':'2'}
        })
        await sleep(250) //wait for message to be delivered

        //stop consuming from qps_key_test1 so we can test priority
        await exchange.cancelConsumer('qps_key_test1')
        //queue some message to test priority
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content2'), {
            priority:1, headers:{'qps-key':'test1','qps-delay':'10', 'qps-max-priority':'2'}
        })
        await sleep(250) //sleep to ensure message order cause awaiting a publish does not confirm without rabbit extension/mode
        await ch.publish('qps_exchange', 'destination_test1', new Buffer('content1'), {
            priority:2, headers:{'qps-key':'test1','qps-delay':'10', 'qps-max-priority':'2'}
        })
        await sleep(250) //wait for messages to be published
        //restart consumer
        await exchange.consumeQpsQueue('test1')

        //read the messages and assert stuff
        var [_, msg1, msg2] = await receiveXMessages(ch, 'destination_test1', 3)
        assert.equal(msg1.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg1.content.toString(), 'content1') //assert content is preserved
        assert.equal(msg2.properties.headers['qps-key'], 'test1') //assert headers are preserved
        assert.equal(msg2.content.toString(), 'content2') //assert content is preserved
    })

    it('command line should run', async function(){
        await main({
            argv:['init'],
            connection:rabbitConnString,
            management:managementConnString,
            test:true,
        })
        await main({
            argv:['clean'],
            connection:rabbitConnString,
            management:managementConnString,
            test:true,
        })
        await main({
            argv:['start'],
            connection:rabbitConnString,
            management:managementConnString,
            test:true,
        })
    })

    it('deleteIdleQueues should not explode', async function(){
        await exchange.deleteIdleQueues()
    }).timeout(15000)
})
