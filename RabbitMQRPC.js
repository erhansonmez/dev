const amqplib = require("amqplib");
const crypto = require("crypto");

function RabbitMQRPC(connectionString) {
  console.log("[x] RabbitMQRPC initialized");
  this.connectionString =
    connectionString ?? "amqp://test:password@192.168.1.42:5672";
  this.connection = null;
}

RabbitMQRPC.prototype.createChannel = async function () {
  if (this.connection === null) {
    console.log("[x] Created new connection");
    this.connection = await amqplib.connect(this.connectionString);
  }
  console.log("[x] Use connection through current");
  return await this.connection.createChannel();
};

RabbitMQRPC.prototype.observer = async function (queue, operation) {
  console.log("[x] Observer started for Queue:" + queue);
  const channel = await this.createChannel();
  await channel.assertQueue(queue, { durable: false });
  channel.prefetch(1000);
  channel.consume(
    queue,
    async (message) => {
      console.log("[x] Observer::Consume queue:" + queue);
      console.log(queue);
      if (message.content) {
        /* DB Operations */
        const payload = JSON.parse(message.content.toString());
        const response = await operation(payload);

        channel.sendToQueue(
          message.properties.replyTo,
          this.buffer(JSON.stringify(response)),
          {
            correlationId: message.properties.correlationId,
          }
        );
        channel.ack(message);
      }
    },
    {
      noAck: false,
    }
  );
};

RabbitMQRPC.prototype.requestData = async function (queue, payload, id) {
  const channel = await this.createChannel();
  const q = await channel.assertQueue("", { 
    exclusive: true,
    autoDelete: true
   });
  channel.sendToQueue(queue, this.buffer(JSON.stringify(payload)), {
    replyTo: q.queue,
    correlationId: id,
  });

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      console.log("[x] requestData::Channel close:" + queue);
      channel.close();
      resolve("API could not fullfil the request");
    }, 8000);
    channel.consume(
      q.queue,
      (message) => {
        console.log("[x] requestData::Consume queue:" + queue);
        if (message.properties.correlationId === id) {
          resolve(JSON.parse(message.content.toString()));
          clearTimeout(timeout);
          channel.close();

        } else {
          reject("Data not found");
        }
      },
      {
        noAck: true,
      }
    );
  });
};

RabbitMQRPC.prototype.request = async function (queue, payload) {
  return await this.requestData(queue, payload, crypto.randomUUID());
};

RabbitMQRPC.prototype.buffer = function (payload) {
  return Buffer.from(payload);
};

module.exports = {
  RabbitMQRPC: new RabbitMQRPC(),
};
