"""
Laboratorio 9 - Redes
Consumidor de datos de estación meteorológica
Consume mensajes de Kafka y grafica en tiempo real
"""

import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from collections import deque

# ============ CONFIGURACIÓN ============
KAFKA_SERVER = "iot.redesuvg.cloud:9092"
KAFKA_TOPIC = '22787'  # Tu carné

# Almacenar últimos 50 datos
temperaturas = deque(maxlen=50)
humedades = deque(maxlen=50)
direcciones_viento = deque(maxlen=50)

# Mapeo de direcciones a números para graficar
DIRECCIONES = {'N': 0, 'NO': 1, 'O': 2, 'SO': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}
DIRECCIONES_NOMBRES = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

# ============ CREAR CONSUMIDOR ============
print("Conectando a Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='latest',
    group_id='mi-grupo',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print(f"✓ Conectado al topic: {KAFKA_TOPIC}\n")

# ============ CONFIGURAR GRÁFICOS ============
plt.ion()  # Modo interactivo
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
fig.suptitle('Estación Meteorológica', fontsize=14, fontweight='bold')

contador = 0

# ============ LOOP PRINCIPAL ============
print("Esperando mensajes...\n")

try:
    while True:
        # Obtener mensajes (espera 1 segundo)
        mensajes = consumer.poll(timeout_ms=1000)
        
        # Procesar cada mensaje
        for topic_partition, records in mensajes.items():
            for msg in records:
                # Extraer datos
                data = msg.value
                temp = data['temperatura']
                hum = data['humedad']
                viento = data['direccion_viento']
                
                # Guardar datos
                temperaturas.append(temp)
                humedades.append(hum)
                direcciones_viento.append(DIRECCIONES[viento])
                
                contador += 1
                
                # Mostrar en consola
                print(f"[Mensaje #{contador}]")
                print(f"  Temp: {temp}°C | Humedad: {hum}% | Viento: {viento}")
                
                # ============ ACTUALIZAR GRÁFICOS ============
                if len(temperaturas) > 0:
                    x = list(range(len(temperaturas)))
                    
                    # Limpiar gráficos
                    ax1.clear()
                    ax2.clear()
                    ax3.clear()
                    
                    # GRÁFICO 1: Temperatura
                    ax1.plot(x, list(temperaturas), 'r-o', linewidth=2, markersize=4)
                    ax1.set_ylabel('Temperatura (°C)')
                    ax1.set_title('Temperatura')
                    ax1.grid(True, alpha=0.3)
                    ax1.set_ylim(0, 110)
                    
                    # GRÁFICO 2: Humedad
                    ax2.plot(x, list(humedades), 'b-s', linewidth=2, markersize=4)
                    ax2.set_ylabel('Humedad (%)')
                    ax2.set_title('Humedad')
                    ax2.grid(True, alpha=0.3)
                    ax2.set_ylim(0, 100)
                    
                    # GRÁFICO 3: Viento
                    ax3.scatter(x, list(direcciones_viento), s=80, c='green', marker='D')
                    ax3.set_ylabel('Dirección')
                    ax3.set_xlabel('Número de Lectura')
                    ax3.set_title('Dirección del Viento')
                    ax3.set_yticks([0, 1, 2, 3, 4, 5, 6, 7])
                    ax3.set_yticklabels(DIRECCIONES_NOMBRES)
                    ax3.grid(True, alpha=0.3, axis='x')
                    
                    plt.tight_layout()
                    plt.pause(0.01)
        
except KeyboardInterrupt:
    print("\n\nDeteniendo consumidor...")
finally:
    consumer.close()
    plt.close()
    print("✓ Cerrado correctamente")