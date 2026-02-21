require("dotenv").config();
const express = require("express");
const smsQueue = require("./smsQueue");

const app = express();
app.use(express.json());

/**
 * Ajouter un SMS dans la queue
 */
app.post("/send-sms", async (req, res) => {
  const { phoneNumber, message } = req.body;

  const job = await smsQueue.add(
    { phoneNumber, message },
    {
      attempts: 3,
      backoff: 5000,
      removeOnComplete: true,
    }
  );

  res.json({ status: "queued", jobId: job.id });
});

/**
 * Monitoring queue
 */
app.get("/queue-status", async (req, res) => {
  const counts = await smsQueue.getJobCounts();

  res.json({
    waiting: counts.waiting,
    active: counts.active,
    completed: counts.completed,
    failed: counts.failed,
  });
});

app.listen(3000, () => {
  console.log("🌍 API SMS lancée sur port 3000");
});