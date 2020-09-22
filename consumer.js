const kafka = require('./kafka');
const reader = require('./kafkaReader');
const Bus = require('./sequentialBus');
const sequentialBus = new Bus();


const consumer = kafka.consumer({ groupId: 'test-group' })

const consume = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic-2', fromBeginning: true })
    
    await reader.connectToBus(sequentialBus, consumer);
}

consume();
