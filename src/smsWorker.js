const smpp = require('smpp');
const Queue = require('bull');

const REDIS_URL = 'redis://ussd-redis:6379';
const smsQueue = new Queue('sms_queue', REDIS_URL);

const SMPP_CONFIG = {
  host: 'messaging.airtel.cd',
  port: 9001,
  system_id: 'AirtelQuiz',
  password: 'Quiz@999',
  source_addr: 'AirtelQuiz',
};

const smppSessions = [];
let sessionIndex = 0;

const reconnectSession = (session, index) => {
  setTimeout(() => {
    console.log(`Tentative de reconnexion de la session SMPP ${index + 1}`);
    session.connect();
  }, 5000);
};

const getAvailableSession = () => {
  let attempts = 0;
  while (attempts < smppSessions.length) {
    const session = smppSessions[sessionIndex];
    if (!session.closed) {
      sessionIndex = (sessionIndex + 1) % smppSessions.length;
      return session;
    }
    attempts++;
    sessionIndex = (sessionIndex + 1) % smppSessions.length;
  }
  return null;
};

// Envoi d'un SMS avec delivery receipt et timeout
const sendSMS = (session, phoneNumber, message, timeout = 30000) => {
  return new Promise((resolve, reject) => {
    if (!session) return reject(new Error('Aucune session SMPP disponible'));
    if (!phoneNumber) return reject(new Error('Numéro vide'));
    if (!message) return reject(new Error('Message vide'));

    const cleanMessage = removeAccents(truncateString(`${message}`, 160));

    let timeoutId;

    const handleDeliveryReceipt = (pdu) => {
      if (pdu.esm_class === 4) {
        const receipt = pdu.short_message.toString();
        if (receipt.includes('stat:DELIVRD')) {
          clearTimeout(timeoutId);
          session.removeListener('deliver_sm', handleDeliveryReceipt);
          console.log(`Delivery receipt confirmé pour +${phoneNumber}`);
          resolve(true);
        } else {
          clearTimeout(timeoutId);
          session.removeListener('deliver_sm', handleDeliveryReceipt);
          console.error(`Delivery failed pour +${phoneNumber}: ${receipt}`);
          reject(new Error(`Delivery failed: ${receipt}`));
        }
      }
    };

    session.on('deliver_sm', handleDeliveryReceipt);

    timeoutId = setTimeout(() => {
      session.removeListener('deliver_sm', handleDeliveryReceipt);
      reject(new Error(`Timeout delivery receipt pour +${phoneNumber}`));
    }, timeout);

    try {
      session.submit_sm({
        source_addr: SMPP_CONFIG.source_addr || 'AirtelQuiz',
        service_type: '',
        source_addr_ton: 5,
        source_addr_npi: 0,
        dest_addr_ton: 1,
        dest_addr_npi: 1,
        destination_addr: `+${phoneNumber}`,
        short_message: cleanMessage,
        registered_delivery: 1,
      }, (pdu) => {
        if (!pdu) return reject(new Error('PDU vide reçu'));
        if (pdu.command_status !== 0) {
          clearTimeout(timeoutId);
          session.removeListener('deliver_sm', handleDeliveryReceipt);
          reject(new Error(`Failed to send SMS: ${pdu.command_status}`));
        } else {
          console.log(`SMS soumis à +${phoneNumber}, attente du delivery receipt...`);
        }
      });
    } catch (err) {
      clearTimeout(timeoutId);
      session.removeListener('deliver_sm', handleDeliveryReceipt);
      reject(err);
    }
  });
};

// Envoi séquentiel d'un tableau de messages
const sendMultipleSMS = async (phoneNumber, messages) => {
  for (let i = 0; i < messages.length; i++) {
    const session = getAvailableSession();
    if (!session) throw new Error('Aucune session SMPP disponible');

    try {
      await sendSMS(session, phoneNumber, messages[i]);
      console.log(`SMS ${i + 1}/${messages.length} confirmé pour +${phoneNumber}`);
    } catch (err) {
      console.error(`Échec de l'envoi du SMS ${i + 1} à +${phoneNumber}:`, err.message);
      // Option: stoppe la séquence en cas d'erreur
      // throw err;
    }
  }
};

// Worker Bull
smsQueue.process(10, async (job, done) => {
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

// Création du pool SMPP
for (let i = 0; i < 10; i++) {
  const session = new smpp.Session({
    host: SMPP_CONFIG.host,
    port: SMPP_CONFIG.port,
    debug: true,
    auto_enquire_link_period: 10000,
    connectTimeout: 20000,
  });

  session.on('connect', () => {
    console.log(`Session SMPP ${i + 1} connectée`);
    session.bind_transceiver({
      system_id: SMPP_CONFIG.system_id,
      password: SMPP_CONFIG.password,
    });
  });

  session.on('bind_transceiver', (pdu) => {
    if (pdu.command_status === 0) {
      console.log(`Session SMPP ${i + 1} liée avec succès`);
    }
  });

  session.on('close', () => {
    console.log(`Session SMPP ${i + 1} fermée`);
    reconnectSession(session, i);
  });

  session.on('error', (err) => {
    console.log(`Erreur sur la session SMPP ${i + 1}:`, err);
    reconnectSession(session, i);
  });

  smppSessions.push(session);
}

console.log('🚀 Worker SMPP démarré...');