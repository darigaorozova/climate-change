import pandas as pd
import pickle
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Импортируем загрузчик
from ml_model.data_loader import load_data_from_clickhouse


# Путь для сохранения модели
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'weather_model.pkl')

def train():
    # 1. Загружаем данные
    df = load_data_from_clickhouse()
    
    if df.empty:
        print("❌ Ошибка: Нет данных для обучения.")
        return

    # 2. Разделение на Features (X) и Target (y)
    X = df.drop(columns=['temperature_c'])
    y = df['temperature_c']
    
    print(f"\nПризнаки для обучения: {list(X.columns)}")

    # 3. Разделение на Train/Test
    # Важно: Для временных рядов лучше не перемешивать (shuffle=False),
    # чтобы мы учились на прошлом и предсказывали будущее.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    print(f"Размер обучающей выборки: {len(X_train)}")
    print(f"Размер тестовой выборки: {len(X_test)}")

    # 4. Обучение модели
    print("\n🚀 Запуск обучения RandomForestRegressor...")
    # n_estimators=100 - количество деревьев
    # n_jobs=-1 - использовать все ядра процессора
    model = RandomForestRegressor(n_estimators=100, max_depth=15, n_jobs=-1, random_state=42)
    model.fit(X_train, y_train)
    
    print("✅ Обучение завершено.")

    # 5. Оценка качества
    print("\n--- ОЦЕНКА МОДЕЛИ ---")
    predictions = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    
    print(f"MAE (Средняя ошибка в градусах): {mae:.2f} °C")
    print(f"R2 Score (Точность): {r2:.4f}")
    
    if r2 > 0.9:
        print("🌟 Отличный результат!")
    elif r2 > 0.7:
        print("👍 Хороший результат.")
    else:
        print("⚠️ Результат так себе, нужно больше данных.")

    # 6. Сохранение модели
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    
    print(f"\n💾 Модель сохранена в: {MODEL_PATH}")

if __name__ == "__main__":
    train()