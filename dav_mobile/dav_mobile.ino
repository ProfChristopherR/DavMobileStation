#include <WiFi.h>
#include <HTTPClient.h>
#include <Wire.h>
#include <SPI.h>
#include <DHT.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BMP280.h>
#include <LiquidCrystal_I2C.h>
#include <time.h>

// =============================================================================
// SISTEMA DAV MOBILE V4.5 — ESTACIÓN METEOROLÓGICA PORTÁTIL
// Liceo Bicentenario de Excelencia Polivalente San Nicolás
// Sensores: DHT22 (Temp/Hum) + BMP280 en SPI (Presión) + Anemómetro Óptico (Viento)
// Hardware: LCD en I2C (GPIO 21/22) | BMP280 en SPI (GPIO 25/26/27/32)
// =============================================================================

// --- CONFIGURACIÓN WIFI CON PRIORIDAD Y RESPALDO ---
const char* WIFI_SSID_PRIORITARIO = "geogo_IoT";
const char* WIFI_PASS_PRIORITARIO = "2iot2025";

const char* WIFI_SSID_RESPALDO = "Bicentenario";
const char* WIFI_PASS_RESPALDO = "";

const char* serverName = "https://services4.arcgis.com/RZghiOOKLo2VeJGO/arcgis/rest/services/Estacion_Metereologica/FeatureServer/0/addFeatures";

// --- ASIGNACIÓN DE PINES DE HARDWARE ---
#define DHTPIN 13             // Pin de datos del DHT22
#define DHTTYPE DHT22
#define ENCODER_PIN 18        // Pin OUT del anemómetro
#define LED_STATUS 2

// Pines BMP280 en modo SPI (100% aislado de los 5V de la pantalla LCD)
#define BMP_SCK 25            // Conectar al pin SCL del sensor
#define BMP_SDI 26            // Conectar al pin SDA del sensor
#define BMP_SDO 27            // Conectar al pin SDO del sensor
#define BMP_CS  32            // Conectar al pin CSB del sensor

DHT dht(DHTPIN, DHTTYPE);
LiquidCrystal_I2C lcd(0x27, 16, 2);                      // LCD en I2C nativo (Pines 21 y 22)
Adafruit_BMP280 bmp(BMP_CS, BMP_SDI, BMP_SDO, BMP_SCK); // BMP280 en modo SPI

bool bmpDisponible = false;

// --- BUFFER CIRCULAR OFFLINE ---
struct Medicion {
  float temp;
  float hum;
  float presion;
  float viento;
  long long timestamp;
};
const int BUFFER_SIZE = 100;
Medicion buffer[BUFFER_SIZE];
int head = 0, tail = 0, count = 0;

// --- VARIABLES ---
volatile int pulses = 0;
static volatile unsigned long debounce = 0;
unsigned long tiempoUltimoCalculo = 0;
unsigned long tiempoUltimaLecturaDHT = 0;
int segundosContados = 0;
float sumaVelocidades = 0;
float ultimaTemp = NAN;
float ultimaHum = NAN;
const char* TZ_INFO = "<-04>4<-03>,M9.1.6/24,M4.1.6/24"; // Horario oficial Chile continental

void IRAM_ATTR counter() {
  if (digitalRead(ENCODER_PIN) && (micros() - debounce > 500)) {
    debounce = micros();
    pulses++;
  }
}

void pestañeoLED() {
  digitalWrite(LED_STATUS, HIGH);
  delay(30);
  digitalWrite(LED_STATUS, LOW);
}

// Lectura del DHT22 cada 2 segundos con persistencia del último valor válido
void leerDHT22() {
  if (millis() - tiempoUltimaLecturaDHT < 2000) return;
  tiempoUltimaLecturaDHT = millis();

  float t = dht.readTemperature();
  float h = dht.readHumidity();

  if (!isnan(t) && t >= -40.0 && t <= 80.0) ultimaTemp = t;
  if (!isnan(h) && h >= 0.0 && h <= 100.0)  ultimaHum = h;
}

// Lectura de presión atmosférica del BMP280
float leerPresionBMP() {
  if (!bmpDisponible) return NAN;
  float p = bmp.readPressure() / 100.0F; // Pa -> hPa
  if (p >= 600.0 && p <= 1150.0) return p;
  return NAN;
}

// Envío a ArcGIS Online con reconexión automática
bool procesarEnvio(float t, float h, float p, float v, long long ts, const char* tipo) {
  if (isnan(t) || isnan(h) || isnan(v)) {
    Serial.printf("[%s] ERROR: lectura NaN detectada. Envio cancelado.\n", tipo);
    return false;
  }
  if (ts < 1700000000000LL) {
    Serial.printf("[%s] ERROR: reloj NTP no sincronizado.\n", tipo);
    return false;
  }

  // Si se cayó la red, intentar reconectar a la prioritaria o al respaldo
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[WIFI] Conexion perdida. Reintentando geogo_IoT...");
    WiFi.disconnect();
    WiFi.begin(WIFI_SSID_PRIORITARIO, WIFI_PASS_PRIORITARIO);
    unsigned long tr = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - tr < 3000) delay(200);

    if (WiFi.status() != WL_CONNECTED) {
      WiFi.disconnect();
      WiFi.begin(WIFI_SSID_RESPALDO, WIFI_PASS_RESPALDO);
      tr = millis();
      while (WiFi.status() != WL_CONNECTED && millis() - tr < 3000) delay(200);
    }

    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[WIFI] Sin conexion a red.");
      return false;
    }
  }

  HTTPClient http;
  http.begin(serverName);
  http.setConnectTimeout(2000);
  http.setTimeout(3000);
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");

  // Armado del JSON para ArcGIS Online
  String data = "f=json&features=[{\"attributes\":{";
  data += "\"temperatura\":" + String(t, 2) + ",";
  data += "\"humedad\":" + String(h, 2) + ",";
  if (!isnan(p)) {
    data += "\"presion\":" + String(p, 2) + ",";
  }
  data += "\"velocidad_viento\":" + String(v, 2) + ",";
  data += "\"fecha_lectura\":" + String(ts);
  data += "}}]";

  Serial.printf("[%s] T:%.1f H:%.1f P:%s V:%.1f ... ", tipo, t, h,
                isnan(p) ? "--" : String(p, 2).c_str(), v);

  int httpCode = http.POST(data);
  bool success = false;

  if (httpCode == HTTP_CODE_OK) {
    String response = http.getString();
    if (response.indexOf("success\":true") > -1) {
      Serial.println("OK (ArcGIS recibio correctamente)");
      success = true;
      pestañeoLED();
    } else {
      Serial.println("ERROR JSON ArcGIS: " + response);
    }
  } else {
    Serial.printf("ERROR HTTP: %d - %s\n", httpCode, http.errorToString(httpCode).c_str());
  }

  http.end();
  return success;
}

void bienvenidaImpacto() {
  String l2 = "Polivalente San Nicolas";
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print("DAV Mobile");
  lcd.setCursor(0, 1); lcd.print("Station v4.4");
  delay(1500);
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print("Liceo Excelencia");
  for (int i = 0; i <= (int)l2.length() - 16; i++) {
    lcd.setCursor(0, 1);
    lcd.print(l2.substring(i, i + 16));
    delay(150);
  }
  delay(400);
}

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println("\n\n=================================================");
  Serial.println("  DAV MOBILE V4.5 (LCD EN I2C + BMP280 EN SPI)");
  Serial.println("=================================================");

  pinMode(LED_STATUS, OUTPUT);

  // 1. Iniciar bus I2C nativo exclusivamente para la Pantalla LCD (Pines 21 y 22)
  Wire.begin(21, 22);
  Wire.setClock(100000);
  lcd.init();
  lcd.backlight();
  bienvenidaImpacto();

  // 2. Iniciar DHT22 en GPIO 13
  dht.begin();
  Serial.println("[DHT22] Sensor de temperatura/humedad iniciado en GPIO 13.");

  // 3. Iniciar BMP280 en modo SPI (Pines 25, 26, 27, 32 - Totalmente aislado de los 5V de la LCD)
  if (bmp.begin()) {
    bmp.setSampling(Adafruit_BMP280::MODE_NORMAL,
                    Adafruit_BMP280::SAMPLING_X2,
                    Adafruit_BMP280::SAMPLING_X16,
                    Adafruit_BMP280::FILTER_X16,
                    Adafruit_BMP280::STANDBY_MS_500);
    bmpDisponible = true;
    Serial.println("[BMP280] Sensor de presion OK en modo SPI (Pines 25/26/27/32).");
  } else {
    Serial.println("[BMP280] ADVERTENCIA: No respondio en SPI. Se continuara sin presion.");
  }

  // 5. Conexión WiFi con Prioridad (1: geogo_IoT, 2: Bicentenario)
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print("WiFi Prioritario");
  lcd.setCursor(0, 1); lcd.print("geogo_IoT...");
  Serial.println("[WIFI] Intentando conectar a: geogo_IoT...");

  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  delay(100);
  WiFi.begin(WIFI_SSID_PRIORITARIO, WIFI_PASS_PRIORITARIO);
  unsigned long startWiFi = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startWiFi < 8000) {
    delay(400);
    Serial.print(".");
  }

  // Si no conectó al prioritario, intentar con respaldo
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("\n[WIFI] geogo_IoT no disponible. Conectando a respaldo: Bicentenario...");
    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("WiFi Respaldo");
    lcd.setCursor(0, 1); lcd.print("Bicentenario...");

    WiFi.disconnect();
    delay(100);
    WiFi.begin(WIFI_SSID_RESPALDO, WIFI_PASS_RESPALDO);
    startWiFi = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startWiFi < 8000) {
      delay(400);
      Serial.print(".");
    }
  }

  lcd.clear();
  if (WiFi.status() == WL_CONNECTED) {
    lcd.setCursor(0, 0); lcd.print("WiFi Conectado!");
    lcd.setCursor(0, 1); lcd.print(WiFi.SSID().substring(0, 16));
    Serial.printf("\n[WIFI] Conectado exitosamente a: %s (IP: %s)\n",
                  WiFi.SSID().c_str(), WiFi.localIP().toString().c_str());
  } else {
    lcd.setCursor(0, 0); lcd.print("WiFi ERROR!");
    lcd.setCursor(0, 1); lcd.print("Modo Offline");
    Serial.println("\n[WIFI] Sin conexion a ninguna red. Modo offline activo.");
  }
  delay(1200);

  // 6. Sincronización NTP
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  setenv("TZ", TZ_INFO, 1);
  tzset();
  struct tm timeinfo;
  for (int i = 0; i < 12 && !getLocalTime(&timeinfo); i++) {
    delay(400);
  }

  // 7. Configuración Anemómetro
  pinMode(ENCODER_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(ENCODER_PIN), counter, RISING);
  tiempoUltimoCalculo = millis();

  lcd.clear();
}

void loop() {
  leerDHT22();

  unsigned long ahora = millis();
  unsigned long dt_ms = ahora - tiempoUltimoCalculo;
  if (dt_ms < 1000) return;

  // Captura atómica de pulsos con tiempo transcurrido exacto
  noInterrupts();
  int pulsosCapturados = pulses;
  pulses = 0;
  interrupts();
  tiempoUltimoCalculo = ahora;

  float dt_seg = (float)dt_ms / 1000.0;
  float rps = ((float)pulsosCapturados / 20.0) / dt_seg;
  float v_inst = rps * (2.0 * 3.1416 * 0.0635) * 2.5 * 3.6; // km/h

  float t = ultimaTemp;
  float h = ultimaHum;
  float p = leerPresionBMP();

  // Actualización LCD 16x2
  char l1[20], l2[20];
  // Línea 1: Temperatura y Humedad (+ Buffer si existe)
  if (count > 0) {
    snprintf(l1, sizeof(l1), "T:%4.1f H:%2.0f B:%02d", t, h, count);
  } else {
    snprintf(l1, sizeof(l1), "T:%4.1fC H:%2.0f%%   ", t, h);
  }

  // Línea 2: Viento y Presión atmosférica en hPa
  if (isnan(p)) {
    snprintf(l2, sizeof(l2), "V:%4.1f P:--    ", v_inst);
  } else {
    snprintf(l2, sizeof(l2), "V:%4.1f P:%5.1f ", v_inst, p);
  }

  l1[16] = '\0';
  l2[16] = '\0';
  lcd.setCursor(0, 0); lcd.print(l1);
  lcd.setCursor(0, 1); lcd.print(l2);

  sumaVelocidades += v_inst;
  segundosContados++;

  // Ciclo de transmisión cada 5 segundos
  if (segundosContados >= 5) {
    float promV = sumaVelocidades / (float)segundosContados;
    long long ts = (long long)time(NULL) * 1000LL;

    if (procesarEnvio(t, h, p, promV, ts, "REALTIME")) {
      // Vaciar cola offline acumulada
      int lote = 0;
      while (count > 0 && lote < 5 && WiFi.status() == WL_CONNECTED) {
        Medicion m = buffer[tail];
        if (!procesarEnvio(m.temp, m.hum, m.presion, m.viento, m.timestamp, "BUFFER")) break;
        tail = (tail + 1) % BUFFER_SIZE;
        count--;
        lote++;
        delay(50);
      }
    } else if (!isnan(t) && !isnan(h) && ts >= 1700000000000LL) {
      if (count == BUFFER_SIZE) {
        tail = (tail + 1) % BUFFER_SIZE;
        count--;
      }
      buffer[head] = { t, h, p, promV, ts };
      head = (head + 1) % BUFFER_SIZE;
      count++;
      Serial.printf("[BUFFER] Guardado offline: %d/%d\n", count, BUFFER_SIZE);
    }

    sumaVelocidades = 0;
    segundosContados = 0;
  }
}
