require("dotenv").config();
const express = require("express");
const smsQueue = require("./smsQueue");

const { createBullBoard } = require("@bull-board/api");
const { BullAdapter } = require("@bull-board/api/bullAdapter");
const { ExpressAdapter } = require("@bull-board/express");

const app = express();
app.use(express.json());

/**
 * ===============================
 * 📊 Configuration Bull Dashboard
 * ===============================
 */
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [new BullAdapter(smsQueue)],
  serverAdapter,
});

/**
 * 🔐 PROTECTION BASIC AUTH
 * 👉 DOIT ÊTRE AVANT getRouter()
 */
app.use("/admin/queues", (req, res, next) => {
  const auth = { login: "admin", password: "1234" };

  const b64auth = (req.headers.authorization || "").split(" ")[1] || "";
  const [login, password] = Buffer.from(b64auth, "base64")
    .toString()
    .split(":");

  if (login === auth.login && password === auth.password) {
    return next();
  }

  res.set("WWW-Authenticate", 'Basic realm="401"');
  res.status(401).send("Authentication required.");
});

/**
 * 📊 Dashboard route
 */
app.use("/admin/queues", serverAdapter.getRouter());

/**
 * Ajouter un SMS dans la queue
 */
app.post("/send-sms", async (req, res) => {
  const { phoneNumber, message } = req.body;

  const job = await smsQueue.add(
    { phoneNumber, message, meta:{type:'message'}},
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