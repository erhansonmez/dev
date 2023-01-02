const amqplib = require("amqplib");
const crypto = require("crypto");
const { resourceLimits } = require("worker_threads");

function RabbitMq(connectionString) {
  console.log("[x] RabbitMq initialized");
  this.connectionString =
    connectionString ?? "amqp://test:password@192.168.1.42:5672";
  this.connection = null;
}

RabbitMq.prototype.createChannel = async function () {
  if (this.connection === null) {
    console.log("[x] Created new connection");
    this.connection = await amqplib.connect(this.connectionString);
  }
  console.log("[x] Use connection through current");
  return await this.connection.createChannel();
};

RabbitMq.prototype.createConsumer = async function (queue, operation, queueOpts) {
  console.log("[x] createConsumer started for Queue:" + queue);
  const channel = await this.createChannel();
  await channel.assertQueue(queue, queueOpts);
  await channel.prefetch(50);
  await channel.consume(queue, operation, {
    noAck: true,
  });
};

RabbitMq.prototype.observer = async function (queue, operation) {
  console.log("[x] Observer started for Queue:" + queue);
  const channel = await this.createChannel();
  await channel.assertQueue(queue, { durable: false });
  await channel.prefetch(50);
  await channel.consume(
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

RabbitMq.prototype.requestRpcData = async function (queue, payload, id) {
  const channel = await this.createChannel();
  const q = await channel.assertQueue("", {
    exclusive: true,
    autoDelete: true,
  });
  channel.sendToQueue(queue, this.buffer(JSON.stringify(payload)), {
    replyTo: q.queue,
    correlationId: id,
  });

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(async () => {
      console.log("[x] requestData::Channel close:" + queue);
      await channel.close();
      resolve("API could not fullfil the request");
    }, 8000);
    channel.consume(
      q.queue,
      async (message) => {
        console.log("[x] requestData::Consume queue:" + queue);
        if (message.properties.correlationId === id) {
          resolve(JSON.parse(message.content.toString()));
          clearTimeout(timeout);
          await channel.close();
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

RabbitMq.prototype.requestData = async function (queue, payload, queueOpts, senderOpts) {
  const channel = await this.createChannel();
  await channel.assertQueue(queue, queueOpts);
  const result = channel.sendToQueue(queue, this.buffer(JSON.stringify(payload)), senderOpts);
  await channel.close();
  return result;
};

RabbitMq.prototype.requestRpc = async function (queue, payload) {
  return await this.requestRpcData(queue, payload, crypto.randomUUID());
};

RabbitMq.prototype.buffer = function (payload) {
  return Buffer.from(payload);
};

RabbitMq.prototype.close = async function () {
  await this.connection.close();
};

module.exports = {
  RabbitMq: new RabbitMq(),
};
