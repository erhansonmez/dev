const { RabbitMQRPC } = require("./RabbitMQRPC");

const RunRabbitMQRPC = async () => {
  const queue = {
    CUSTOMER_RPC: "CUSTOMER_RPC",
  };

  // Response
  RabbitMQRPC.observer(queue.CUSTOMER_RPC, async (payload) => {
    // DB Operations or stuff
    console.log(payload);
    return new Promise((resolve, reject) => setTimeout(resolve({ message: "test response" }), 3000)); // Wait 3000ms before send response
  });


  // Request
  const result = await RabbitMQRPC.request(queue.CUSTOMER_RPC, { id: 1 });

  console.log( result );
}

RunRabbitMQRPC();
// { message: "test response" }
