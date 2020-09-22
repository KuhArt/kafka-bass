const connectToBus = (bus, consumer) => {
    consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            bus.push({ topic, partition, message });
        },
    })

    const { GROUP_JOIN } = consumer.events

    consumer.on(GROUP_JOIN, () => {
        bus.flush();
    })

    bus.on('maxQueueSizeReached', () => {
        consumer.pause();
    });

    bus.on('empty', () => {
        consumer.resume();
    });

    bus.on('processed', (event) => {

    })

    
    return consumer;
}

module.exports.connectToBus = connectToBus;