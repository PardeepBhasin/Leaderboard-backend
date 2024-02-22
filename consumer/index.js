const express = require('express');
const Redis = require('ioredis');
const Server = require("socket.io");

const app = express();
app.use(express.json());

const io = Server(app.listen(5000, () => {
    console.log('Consumer running on port 5000');
}), {
    cors: {
        origin: "http://localhost:3000"
    }
})
const client = new Redis({
    host: 'redis-11958.c267.us-east-1-4.ec2.cloud.redislabs.com',
    port: 11958,
    password: 'your password'
});

const streamName = 'myStream';
const groupName = 'consumerGroup';

// XGROUP is used in order to create, destroy and manage consumer groups.
// XREADGROUP is used to read from a stream via a consumer group.
// XACK is the command that allows a consumer to mark a pending message as correctly processed.
async function createConsumerGroupIfNotExist() {
    const groupInfo = await client.xinfo('GROUPS', streamName);
    if (groupInfo.length === 0) {
        console.log("++++++++++++groupInfo", groupInfo);
        client.xgroup("CREATE", streamName, groupName, '$', (err) => {
            if (err) {
                console.log("CREATE++++++++", err);
            }
            console.log(`consumer group is created successfully with name as ${groupName} corresponding to stream ${streamName}`);
        })
    }
}

createConsumerGroupIfNotExist().then(() => {
    listenToMessages();
});

function listenToMessages() {
    client.xreadgroup(
        'GROUP', groupName, 'ConsumerName', 'BLOCK', '100', 'STREAMS', streamName, '>', function (err, result) {
            if (err) {
                console.log("Read++++++++", err);
                setTimeout(() => {
                    listenToMessages();
                }, 1000);
                return;
            }
            if (result && result.length > 0) {
                const stream = result[0];
                const messageId = stream[1][0][0];
                const message = stream[1][0][1];
                console.log("message+++++++++++", message);
                client.xack(streamName, groupName, messageId, async (err) => {
                    if (err) {
                        console.log("Message not processed");
                    } else {
                        const data = await client.hget('players', message[0]);
                        const player = JSON.parse(data);
                        // message[1] contains score and we are aggregating the existing score with the score that we recieved in the message
                        console.log("I am called message++++++", player.score, message[1]);
                        console.log("I am called finally++++++", player.score, message[1]);
                        player.score = player.score + Number(message[1]);
                        console.log("Score++++++++++", player.score);
                        await client.hset('players', `playerId ${player.playerId}`, JSON.stringify(player));
                        io.emit('player-event', player);
                    }
                    listenToMessages();
                });
            } else {
                listenToMessages();
            }
        });
}
// Check group info using xinfo method
// Create Consumer Group xgroup
// Read consumer group
// xack

