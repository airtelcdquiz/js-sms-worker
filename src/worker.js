require("dotenv").config();
const smsQueue = require("./smsQueue");
const { sendSMS } = require("./smppClient.js");

console.log("🚀 Worker SMS démarré...");

smsQueue.process(5, async (job) => {
  const { meta } = job.data;
  try {
    if (meta.type === 'bulk-message') {
      const { phoneNumber, messages } = job.data;
      await sendMultipleSMS(phoneNumber, messages);
      done();
    } else if (meta.type === 'message') {
      const { phoneNumber, message } = job.data;
      const session = getAvailableSession();
      await sendSMS(session, phoneNumber, message);
      done();
    }
  } catch (error) {
    console.error(`Erreur lors de l'envoi du SMS à ${job.data.phoneNumber}:`, error.message);
    done({ message: error.message });
  }
});

smsQueue.on("failed", (job, err) => {
  console.log(`❌ Job ${job.id} échoué:`, err.message);
});