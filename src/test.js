require("dotenv").config();
const smpp = require("smpp");

const SMPP_HOST = process.env.SMPP_HOST || "messaging.airtel.cd";
const SMPP_PORT = Number(process.env.SMPP_PORT || 9001);
const SYSTEM_ID = process.env.SMPP_SYSTEM_ID || "AirtelQuiz";
const PASSWORD = process.env.SMPP_PASSWORD || "@irtElq1";
const SOURCE_ADDR = process.env.SMPP_SOURCE_ADDR || "AirtelQuiz";

const TO = "243970908479"; // ✅ Mets TON numéro au format 243...
const MESSAGE = "Test SMS depuis SMPP! pour Pascal"; // Le message à envoyer

const session = new smpp.Session({
  host: SMPP_HOST,
  port: SMPP_PORT,
  auto_enquire_link_period: 10000,
  connectTimeout: 20000
});

session.on("connect", () => {
  console.log("✅ Connecté au SMSC, bind...");

  session.bind_transmitter(
    { system_id: SYSTEM_ID, password: PASSWORD, interface_version: 1, addr_ton: 5, addr_npi: 1 },
    (pdu) => {
      if (pdu.command_status !== 0) {
        console.log("❌ Bind failed:", pdu.command_status);
        session.close();
        return;
      }

      console.log("✅ Bind OK, envoi du SMS...");

      session.submit_sm(
        {
          source_addr: SOURCE_ADDR,
          source_addr_ton: 5,
          source_addr_npi: 0,
          dest_addr_ton: 1,
          dest_addr_npi: 1,
          destination_addr: TO,
          short_message: MESSAGE,
        },
        (resp) => {
          if (resp.command_status === 0) {
            console.log("📨 SMS envoyé ! message_id =", resp.message_id);
          } else {
            console.log("❌ Échec envoi:", resp.command_status);
          }
          session.close();
        }
      );
    }
  );
});

session.on("error", (e) => console.log("❌ SMPP error:", e.message || e));
session.on("close", () => console.log(" Session fermée"));