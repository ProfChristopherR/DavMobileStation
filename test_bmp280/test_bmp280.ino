#include <Adafruit_BMP280.h>
#include <Adafruit_Sensor.h>
#include <LiquidCrystal_I2C.h>
#include <SPI.h>

// Pantalla LCD en sus pines I2C habituales (21 y 22)
LiquidCrystal_I2C lcd(0x27, 16, 2);

// BMP280 en modo SPI (Software SPI en pines 100% independientes)
#define BMP_SCK 25 // Conectar al pin SCL del sensor
#define BMP_SDI 26 // Conectar al pin SDA del sensor
#define BMP_SDO 27 // Conectar al pin SDO del sensor
#define BMP_CS 32  // Conectar al pin CSB del sensor

Adafruit_BMP280 bmp(BMP_CS, BMP_SDI, BMP_SDO, BMP_SCK);

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println(
      "\n--- TEST: LCD EN I2C (21/22) + BMP280 EN SPI (25/26/27/32) ---");

  // Iniciar LCD
  lcd.init();
  lcd.backlight();
  lcd.setCursor(0, 0);
  lcd.print("TEST MODO SPI");

  // Iniciar BMP280 en SPI
  if (bmp.begin()) {
    Serial.println("[BMP280] ¡ÉXITO! Sensor funcionando en modo SPI.");
    lcd.setCursor(0, 1);
    lcd.print("BMP280 SPI: OK!");
  } else {
    Serial.println("[BMP280] ERROR: No responde en SPI.");
    lcd.setCursor(0, 1);
    lcd.print("BMP SPI: ERROR");
  }
}

void loop() {
  float p = bmp.readPressure() / 100.0F;
  float t = bmp.readTemperature();
  Serial.printf("Temp: %.1f C | Presion: %.1f hPa\n", t, p);

  lcd.setCursor(0, 0);
  lcd.printf("T:%.1fC P:%.1f", t, p);
  delay(1000);
}
