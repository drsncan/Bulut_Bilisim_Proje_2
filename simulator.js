require('dotenv').config(); // .env dosyasındaki değişkenleri yükler
const { EventHubProducerClient } = require("@azure/event-hubs");

// Şifreleri artık doğrudan buraya yazmıyoruz, .env dosyasından çekiyoruz
const connectionString = process.env.EVENT_HUB_CONNECTION_STRING;
const eventHubName = process.env.EVENT_HUB_NAME;

async function main() {
    // Bilgilerin yüklenip yüklenmediğini kontrol edelim (Opsiyonel ama iyi bir pratiktir)
    if (!connectionString || !eventHubName) {
        console.error("HATA: .env dosyasındaki bağlantı bilgileri bulunamadı!");
        process.exit(1);
    }

    // Azure Event Hubs'a bağlanacak olan kuryemizi oluşturuyoruz
    const producer = new EventHubProducerClient(connectionString, eventHubName);
    console.log("✅ Azure Event Hubs bağlantısı hazır. Veri akışı başlıyor...");

    // Her 2 saniyede bir yeni veri üret ve Azure'a gönder
    setInterval(async () => {
        try {
            const vitalData = generateVitalData();
            
            // Azure'a veriyi paket halinde göndermemiz gerekiyor
            const eventDataBatch = await producer.createBatch();
            eventDataBatch.tryAdd({ body: vitalData });
            
            // Paketi fırlat
            await producer.sendBatch(eventDataBatch);
            console.log(`📡 Azure'a Gönderilen Veri: ${JSON.stringify(vitalData)}`);
            
        } catch (err) {
            console.log("❌ Veri gönderilirken hata oluştu: ", err);
        }
    }, 2000);
}

// Fizyolojik verileri gerçeğe yakın simüle eden fonksiyon
function generateVitalData() {
    const heartRate = Math.floor(Math.random() * (105 - 60 + 1)) + 60; 
    const spO2 = Math.floor(Math.random() * (100 - 92 + 1)) + 92;      
    const respiratoryRate = Math.floor(Math.random() * (25 - 12 + 1)) + 12; 
    
    // %5 ihtimalle kritik bir durum (anomali) oluştur
    const isAnomaly = Math.random() < 0.05;

    return {
        patientId: 'PT-98765',
        timestamp: new Date().toISOString(),
        metrics: {
            heartRate: isAnomaly ? 130 : heartRate, 
            spO2: isAnomaly ? 88 : spO2,
            respiratoryRate: isAnomaly ? 30 : respiratoryRate
        },
        status: isAnomaly ? 'CRITICAL' : 'STABLE'
    };
}

// Sistemi başlat
main().catch((err) => {
    console.log("⚠️ Ana sistem hatası: ", err);
});