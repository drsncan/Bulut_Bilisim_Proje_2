require('dotenv').config(); // .env dosyasındaki değişkenleri yükler
const { EventHubConsumerClient } = require("@azure/event-hubs");
const { MongoClient } = require("mongodb");

// Şifreleri artık doğrudan buraya yazmıyoruz, .env dosyasından çekiyoruz
const eventHubConnectionString = process.env.EVENT_HUB_CONNECTION_STRING;
const eventHubName = process.env.EVENT_HUB_NAME;
const mongoConnectionString = process.env.MONGO_CONNECTION_STRING;

const consumerGroup = "$Default";
const dbName = "MedicalProjectDB";
const collectionName = "CriticalAlarms";

async function main() {
    // Bilgilerin yüklenip yüklenmediğini kontrol edelim
    if (!eventHubConnectionString || !eventHubName || !mongoConnectionString) {
        console.error("HATA: .env dosyasındaki bağlantı bilgileri (Event Hub veya MongoDB) bulunamadı!");
        process.exit(1);
    }

    // 1. MongoDB'ye (Cosmos DB) Bağlan
    const mongoClient = new MongoClient(mongoConnectionString);
    try {
        await mongoClient.connect();
        console.log("✅ Azure Cosmos DB (MongoDB) veritabanına bağlanıldı!");
    } catch (dbConnError) {
        console.error("❌ Veritabanı bağlantı hatası:", dbConnError);
        process.exit(1);
    }
    
    const db = mongoClient.db(dbName);
    const collection = db.collection(collectionName);

    // 2. Event Hubs'a Bağlan
    const consumerClient = new EventHubConsumerClient(consumerGroup, eventHubConnectionString, eventHubName);
    console.log("✅ Azure Event Hubs dinleniyor. Gelen veriler analiz edilecek...\n");

    // 3. Verileri Dinle ve İşle
    const subscription = consumerClient.subscribe({
        processEvents: async (events, context) => {
            for (const event of events) {
                const data = event.body;
                
                console.log(`[VERİ ALINDI] Zaman: ${data.timestamp} | Kalp: ${data.metrics.heartRate} | Oksijen: %${data.metrics.spO2}`);

                // ANALİZ: Eğer hastanın durumu kritikse ALARM ver ve VERİTABANINA KAYDET
                if (data.status === 'CRITICAL' || data.metrics.spO2 < 90) {
                    console.log(`🚨 DİKKAT! KRİTİK DURUM TESPİT EDİLDİ! 🚨`);
                    console.log(`   -> Müdahale Gerekiyor: Kalp ${data.metrics.heartRate}, SpO2 %${data.metrics.spO2}`);
                    
                    // Veritabanına kaydedilecek alarm objesini oluştur
                    const alarmRecord = {
                        patientId: data.patientId,
                        timestamp: data.timestamp,
                        alertType: "CRITICAL_VITALS",
                        details: `Düşük Oksijen veya Yüksek Nabız: Kalp ${data.metrics.heartRate}, SpO2 %${data.metrics.spO2}`,
                        status: "UNRESOLVED"
                    };

                    // Alarmı Cosmos DB'ye yaz
                    try {
                        const result = await collection.insertOne(alarmRecord);
                        console.log(`💾 ALARM VERİTABANINA KAYDEDİLDİ! Kayıt ID: ${result.insertedId}`);
                    } catch (dbError) {
                        console.error("❌ Veritabanına kayıt sırasında hata:", dbError);
                    }
                    console.log(`-------------------------------------------------`);
                }
            }
        },
        processError: async (err, context) => {
            console.log(`⚠️ Bir hata oluştu: ${err}`);
        }
    });
}

main().catch((err) => {
    console.log("⚠️ Tüketici sistemi hatası: ", err);
});