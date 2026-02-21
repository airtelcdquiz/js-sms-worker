const Queue = require("bull");

const smsQueue = new Queue("sms-queue", {
  redis: {
    host: "ussd-redis",
    port: 6379,
  },
});

module.exports = smsQueue;