require("dotenv").config();
const smsQueue = require("./smsQueue");
const { sendSMS } = require("./smppClient.js");

console.log("🚀 Worker SMS démarré...");

smsQueue.process(5, async (job) => {
  const { phoneNumber, message } = job.data;

  console.log("📩 Envoi vers:", phoneNumber);

  try {
    const messageId = await sendSMS(phoneNumber, message);
    console.log("✅ SMS envoyé:", messageId);

    return { status: "sent", messageId };
  } catch (error) {
    console.error("❌ Erreur envoi SMS:", error);
    throw error; // Bull gère retry
  }
});

smsQueue.on("failed", (job, err) => {
  console.log(`❌ Job ${job.id} échoué:`, err.message);
});