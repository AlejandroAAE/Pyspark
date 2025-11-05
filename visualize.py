import pandas as pd
import matplotlib.pyplot as plt
import os

# Ruta al CSV limpio
PATH = "processed/data/cleaned.csv"

if not os.path.exists(PATH):
    raise FileNotFoundError(f"No se encuentra el archivo: {PATH}")

# Cargar datos limpios
df = pd.read_csv(PATH)

# =============================
#   GRÁFICA 1
#   Distancia vs Precio
# =============================
plt.figure()
plt.scatter(df["trip_distance_km"], df["fare_amount_eur"])
plt.title("Relación entre distancia y precio")
plt.xlabel("Distancia del viaje (km)")
plt.ylabel("Precio (€)")
plt.grid(True)
plt.show()

# =============================
#   GRÁFICA 2
#   Duración del viaje
# =============================
plt.figure()
plt.hist(df["trip_minutes"], bins=10)
plt.title("Distribución de la duración del viaje")
plt.xlabel("Minutos")
plt.ylabel("Número de viajes")
plt.grid(True)
plt.show()
