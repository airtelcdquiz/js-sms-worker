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


// Initialiser 10 sessions SMPP dans un pool
const smppSessions = [];
let sessionIndex = 0;


// Fonction pour reconnecter une session perdue
const reconnectSession = (session, index) => {
  setTimeout(() => {
    console.log(`Tentative de reconnexion de la session SMPP ${index + 1}`);
    session.connect();
  }, 5000); // Essayer de se reconnecter après 5 secondes
};



// Fonction pour obtenir une session disponible
const getAvailableSession = () => {
    let attempts = 0;
    while (attempts < smppSessions.length) {
      const session = smppSessions[sessionIndex];
      if (session.closed === false) {
        sessionIndex = (sessionIndex + 1) % smppSessions.length;  // Dispatcher au prochain
        return session;
      }
      attempts++;
      sessionIndex = (sessionIndex + 1) % smppSessions.length;  // Tenter la session suivante
    }
    return null;
  };

// Fonction pour envoyer un SMS via une session SMPP
const sendSMS = (session, phoneNumber, message) => {
    return new Promise((resolve, reject) => {
      // const session = getAvailableSession();
      
      if (!session) {
        reject(new Error('Aucune session SMPP disponible'));
        return;
      }
  
      session.submit_sm({
        source_addr: 'AirtelQuiz',
        service_type: '',
        source_addr_ton: 5,
        source_addr_npi: 0,
        dest_addr_ton: 1,
        dest_addr_npi: 1,
        destination_addr: `+${phoneNumber}`,
        short_message: removeAccents(truncateString(`${message}`, 160)),
      }, (pdu) => {
        if (pdu.command_status === 0) {
          console.log(`Message envoyé à +${phoneNumber}`);
          resolve(true);
        } else {
          console.log(`Échec de l'envoi à ${phoneNumber}: ${pdu.command_status}`);
          reject(new Error(`Failed to send SMS to ${phoneNumber}`));
        }
      });
    });
  };

// Fonction pour envoyer un tableau de messages à un même numéro
const sendMultipleSMS = async (phoneNumber, messages) => {
  for (let i = 0; i < messages.length; i++) {
    const session = getAvailableSession();
    if (!session) throw new Error('Aucune session SMPP disponible');

    try {
      await sendSMS(session, phoneNumber, messages[i]);
      console.log(`SMS ${i + 1}/${messages.length} envoyé à +${phoneNumber}`);
    } catch (err) {
      console.error(`Échec de l'envoi du SMS ${i + 1} à +${phoneNumber}:`, err.message);
      // Si tu veux arrêter la séquence en cas d'erreur, décommente la ligne suivante
      // throw err;
    }
  }
};

// Processus Bull pour envoyer les SMS
smsQueue.process(10, async (job, done) => {
    const { meta } = job.data ;
    const { phoneNumber, message } = job.data; 

    try {
      if( meta.type == "bulk-message" ){
        const { phoneNumber, messages } = job.data;
        await sendMultipleSMS(phoneNumber, messages);
        done()
      }
      if ( meta.type == "message" ) {
        const session = getAvailableSession();
        // Utilisation de la session SMPP disponible pour l'envoi du SMS
        await sendSMS(session, phoneNumber, message);
        console.log(`SMS envoyé à ${phoneNumber}`);
        console.log(`SMS >> ${message}`);
        done()
      }
    } catch (error) {
      console.error(`Erreur lors de l'envoi du SMS à ${phoneNumber}:`, error.message);
      done({ message: error.message });
    }
});


// Créer un pool de 10 sessions SMPP
for (let i = 0; i < 10; i++) {
  const session = new smpp.Session({ host: 'messaging.airtel.cd', port: 9001, debug: true, auto_enquire_link_period: 10000, connectTimeout: 20000 });

  session.on('connect', () => {
    console.log(`Session SMPP ${i + 1} connectée`);
    session.bind_transceiver({
      system_id: SMPP_CONFIG.system_id,
      password: SMPP_CONFIG.password
    });
  });

  session.on('bind_transceiver', (pdu) => {
    if (pdu.command_status === 0) {
      console.log(`Session SMPP ${i + 1} liée avec succès`);
    }
  });

  session.on('close', () => {
    console.log(`Session SMPP ${i + 1} fermée`);
    // Réessayer de reconnecter la session
    reconnectSession(session, i);
  });

  session.on('error', (err) => {
    console.log(`Erreur sur la session SMPP ${i + 1}:`, err);
    // Réessayer de reconnecter la session
    reconnectSession(session, i);
  });

  smppSessions.push(session);
}