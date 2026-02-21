require("dotenv").config();
const smpp = require("smpp");

const SMPP_HOST = process.env.SMPP_HOST || "messaging.airtel.cd";
const SMPP_PORT = Number(process.env.SMPP_PORT || 9001);
const SYSTEM_ID = process.env.SMPP_SYSTEM_ID || "AirtelQuiz";
const PASSWORD = process.env.SMPP_PASSWORD || "@irtElq1";
const SOURCE_ADDR = process.env.SMPP_SOURCE_ADDR || "AirtelQuiz";

function sendSMS(phoneNumber, message) {
  return new Promise((resolve, reject) => {
    const session = new smpp.Session({
      host: SMPP_HOST,
      port: SMPP_PORT,
      auto_enquire_link_period: 10000,
      connectTimeout: 20000,
    });

    session.on("connect", () => {
      session.bind_transmitter(
        {
          system_id: SYSTEM_ID,
          password: PASSWORD,
          interface_version: 1,
          addr_ton: 5,
          addr_npi: 1,
        },
        (pdu) => {
          if (pdu.command_status !== 0) {
            session.close();
            return reject("Bind failed: " + pdu.command_status);
          }

          session.submit_sm(
            {
              source_addr: SOURCE_ADDR,
              source_addr_ton: 5,
              source_addr_npi: 0,
              dest_addr_ton: 1,
              dest_addr_npi: 1,
              destination_addr: phoneNumber,
              short_message: message,
            },
            (resp) => {
              if (resp.command_status === 0) {
                resolve(resp.message_id);
              } else {
                reject("Submit failed: " + resp.command_status);
              }
              session.close();
            }
          );
        }
      );
    });

    session.on("error", reject);
  });
}

module.exports = { sendSMS };