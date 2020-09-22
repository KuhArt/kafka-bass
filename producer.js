const kafka = require('./kafka');
const producer = kafka.producer()
const produce = async () => {
    await producer.connect()
    await producer.send({
        topic: 'test-topic-2',
        messages: [
            { value: Buffer.from(JSON.stringify({ data: 'Hello KafkaJS user!', type: 'test' })) },
        ],
    })

    await producer.disconnect()
}

produce()
